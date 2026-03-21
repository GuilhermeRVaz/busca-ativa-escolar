import argparse
import logging
import re
from pathlib import Path

import pandas as pd

from config import get_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_DAILY_LEDGER_PATH = Path("relatorios/Daily_Campaign_Ledger.xlsx")


class ResponseReportBuilder:
    def __init__(self, campaign_path: Path, responses_path: Path | None = None) -> None:
        self.campaign_path = campaign_path
        self.responses_path = responses_path
        self.settings = get_settings()

    def run(self, output_path: Path | None = None) -> Path:
        campaign_df = self._load_campaign()
        campaign_id = self._resolve_campaign_id(campaign_df)
        responses_path = self._resolve_responses_path(campaign_id)
        responses_df = self._load_responses(responses_path)
        ledger_df = self._load_ledger()

        enriched_campaign_df = self._prepare_campaign(campaign_df, ledger_df)
        matched_messages_df = self._prepare_messages(responses_df)
        report_parts = self._build_report_parts(enriched_campaign_df, matched_messages_df)

        report_path = Path(
            output_path or self.campaign_path.parent / f"Relatorio_de_Retornos_{campaign_id}.xlsx",
        )
        self._write_report(report_parts, report_path)
        logger.info("Relatorio de retornos salvo em %s", report_path)
        return report_path

    def _load_campaign(self) -> pd.DataFrame:
        if not self.campaign_path.exists():
            raise FileNotFoundError(f"Campanha nao encontrada: {self.campaign_path}")
        return pd.read_excel(self.campaign_path, sheet_name="Campanha")

    def _resolve_campaign_id(self, campaign_df: pd.DataFrame) -> str:
        if not campaign_df.empty and "campaign_id" in campaign_df.columns:
            campaign_id = self._safe_text(campaign_df.iloc[0].get("campaign_id"))
            if campaign_id:
                return campaign_id
        return self.campaign_path.stem

    def _resolve_responses_path(self, campaign_id: str) -> Path:
        if self.responses_path is not None:
            return self.responses_path
        return self.campaign_path.parent / f"WhatsApp_Responses_Normalized_{campaign_id}.xlsx"

    def _load_responses(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Base normalizada de respostas nao encontrada: {path}")
        return pd.read_excel(path, sheet_name="Messages")

    def _load_ledger(self) -> pd.DataFrame:
        ledger_path = self._resolve_ledger_path()
        if not ledger_path.exists():
            logger.warning("Ledger nao encontrado em %s. Relatorio seguira apenas com a campanha.", ledger_path)
            return pd.DataFrame()
        return pd.read_excel(ledger_path, sheet_name="Historico")

    def _resolve_ledger_path(self) -> Path:
        if self.campaign_path.stem.lower().startswith("campanha_diaria_"):
            return DEFAULT_DAILY_LEDGER_PATH
        return self.settings.campaign_ledger_path

    def _prepare_campaign(self, campaign_df: pd.DataFrame, ledger_df: pd.DataFrame) -> pd.DataFrame:
        prepared = campaign_df.copy()
        for column in ["student_name", "parent_name", "phone_sanitized", "ra_key", "contact_slot"]:
            prepared[column] = prepared[column].apply(self._safe_text)
        for column in ["status_envio", "status_resposta", "observacao", "whatsapp_message"]:
            if column in prepared.columns:
                prepared[column] = prepared[column].apply(self._safe_text)
        prepared["data_envio_dt"] = pd.to_datetime(prepared["data_envio"], errors="coerce")
        prepared["campaign_key"] = prepared.apply(self._build_campaign_key, axis=1)
        prepared["whatsapp_message_norm"] = prepared["whatsapp_message"].map(self._normalize_text)

        if ledger_df.empty:
            prepared["ledger_status_resposta"] = ""
            prepared["ledger_observacao"] = ""
            return prepared

        ledger_prepared = ledger_df.copy()
        for column in ["ra_key", "phone_sanitized", "contact_slot", "status_resposta", "observacao"]:
            if column in ledger_prepared.columns:
                ledger_prepared[column] = ledger_prepared[column].apply(self._safe_text)
        ledger_prepared["campaign_key"] = ledger_prepared.apply(self._build_campaign_key, axis=1)
        ledger_trimmed = ledger_prepared[["campaign_key", "status_resposta", "observacao"]].drop_duplicates(
            subset=["campaign_key"],
            keep="last",
        )
        return prepared.merge(
            ledger_trimmed.rename(
                columns={
                    "status_resposta": "ledger_status_resposta",
                    "observacao": "ledger_observacao",
                }
            ),
            on="campaign_key",
            how="left",
        )

    def _prepare_messages(self, responses_df: pd.DataFrame) -> pd.DataFrame:
        prepared = responses_df.copy()
        for column in [
            "matched_ra_key",
            "matched_phone",
            "matched_contact_slot",
            "matched_parent_name",
            "matched_student_name",
            "author_label",
            "message_text",
        ]:
            if column in prepared.columns:
                prepared[column] = prepared[column].apply(self._safe_text)
        prepared["message_datetime_dt"] = pd.to_datetime(prepared["message_datetime"], errors="coerce")
        prepared["campaign_key"] = prepared.apply(
            lambda row: self._join_key(
                row.get("matched_ra_key"),
                row.get("matched_phone"),
                row.get("matched_contact_slot"),
            ),
            axis=1,
        )
        prepared["message_text_norm"] = prepared["message_text"].map(self._normalize_text)
        prepared["matched"] = prepared["matched"].fillna(False).astype(bool)
        prepared = prepared.loc[prepared["matched"]].copy()
        return prepared

    def _build_report_parts(
        self,
        campaign_df: pd.DataFrame,
        messages_df: pd.DataFrame,
    ) -> dict[str, pd.DataFrame]:
        sent_df = campaign_df.loc[campaign_df["status_envio"].str.lower().eq("enviado")].copy()
        responded_rows: list[dict[str, object]] = []
        incoming_rows: list[dict[str, object]] = []

        for _, campaign_row in sent_df.iterrows():
            candidate_messages = messages_df.loc[
                messages_df["campaign_key"].eq(campaign_row["campaign_key"])
                & messages_df["message_datetime_dt"].notna()
            ].copy()
            if pd.notna(campaign_row["data_envio_dt"]):
                candidate_messages = candidate_messages.loc[
                    candidate_messages["message_datetime_dt"] >= campaign_row["data_envio_dt"]
                ]
            candidate_messages = candidate_messages.sort_values("message_datetime_dt")
            incoming_messages = candidate_messages.loc[
                candidate_messages["message_text_norm"].ne(campaign_row["whatsapp_message_norm"])
            ].copy()
            if incoming_messages.empty:
                continue

            first_response = incoming_messages.iloc[0]
            responded_rows.append(
                {
                    "campaign_id": campaign_row["campaign_id"],
                    "class_name": campaign_row["class_name"],
                    "student_name": campaign_row["student_name"],
                    "parent_name": campaign_row["parent_name"],
                    "phone_sanitized": campaign_row["phone_sanitized"],
                    "data_envio": self._safe_text(campaign_row["data_envio"]),
                    "data_primeira_resposta": self._safe_text(first_response["message_datetime"]),
                    "autor_primeira_resposta": self._safe_text(first_response["author_label"]),
                    "texto_primeira_resposta": self._safe_text(first_response["message_text"]),
                    "total_mensagens_recebidas": len(incoming_messages),
                    "status_envio": self._safe_text(campaign_row["status_envio"]),
                    "status_resposta_campanha": self._safe_text(campaign_row.get("status_resposta")),
                    "status_resposta_ledger": self._safe_text(campaign_row.get("ledger_status_resposta")),
                    "observacao": self._safe_text(campaign_row.get("observacao")),
                }
            )
            for _, message_row in incoming_messages.iterrows():
                incoming_rows.append(
                    {
                        "campaign_id": campaign_row["campaign_id"],
                        "class_name": campaign_row["class_name"],
                        "student_name": campaign_row["student_name"],
                        "parent_name": campaign_row["parent_name"],
                        "phone_sanitized": campaign_row["phone_sanitized"],
                        "message_datetime": self._safe_text(message_row["message_datetime"]),
                        "author_label": self._safe_text(message_row["author_label"]),
                        "message_text": self._safe_text(message_row["message_text"]),
                        "source_file": self._safe_text(message_row["source_file"]),
                    }
                )

        responded_df = pd.DataFrame(responded_rows)
        incoming_df = pd.DataFrame(incoming_rows)
        responded_keys = set(responded_df["phone_sanitized"]) if not responded_df.empty else set()

        sem_retorno_df = sent_df.loc[~sent_df["phone_sanitized"].isin(responded_keys)].copy()
        sem_retorno_df = sem_retorno_df[
            [
                "campaign_id",
                "class_name",
                "student_name",
                "parent_name",
                "phone_sanitized",
                "data_envio",
                "status_envio",
                "status_resposta",
                "observacao",
            ]
        ]

        nao_recontatar_df = responded_df[
            [
                "campaign_id",
                "class_name",
                "student_name",
                "parent_name",
                "phone_sanitized",
                "data_envio",
                "data_primeira_resposta",
                "texto_primeira_resposta",
            ]
        ] if not responded_df.empty else pd.DataFrame(
            columns=[
                "campaign_id",
                "class_name",
                "student_name",
                "parent_name",
                "phone_sanitized",
                "data_envio",
                "data_primeira_resposta",
                "texto_primeira_resposta",
            ]
        )

        justificativas_df = incoming_df.copy()

        total_enviados = len(sent_df)
        total_respondidos = len(responded_df)
        total_sem_retorno = len(sem_retorno_df)
        taxa_resposta = round((total_respondidos / total_enviados) * 100, 2) if total_enviados else 0.0
        total_numero_invalido = int(
            campaign_df["status_resposta"].fillna("").astype(str).str.lower().eq("numero_invalido").sum()
        )

        resumo_df = pd.DataFrame(
            [
                {"indicador": "campaign_id", "valor": self._resolve_campaign_id(campaign_df)},
                {"indicador": "total_enviados", "valor": total_enviados},
                {"indicador": "total_respondidos", "valor": total_respondidos},
                {"indicador": "total_sem_retorno", "valor": total_sem_retorno},
                {"indicador": "total_numero_invalido", "valor": total_numero_invalido},
                {"indicador": "taxa_resposta_percentual", "valor": taxa_resposta},
            ]
        )

        return {
            "Resumo": resumo_df,
            "Respondidos": responded_df,
            "Sem_Retorno": sem_retorno_df,
            "Nao_Recontatar": nao_recontatar_df,
            "Justificativas": justificativas_df,
        }

    @staticmethod
    def _write_report(parts: dict[str, pd.DataFrame], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for sheet_name, df in parts.items():
                export_df = df if not df.empty else pd.DataFrame()
                export_df.to_excel(writer, sheet_name=sheet_name, index=False)

    @staticmethod
    def _build_campaign_key(row: pd.Series) -> str:
        return ResponseReportBuilder._join_key(
            row.get("ra_key"),
            row.get("phone_sanitized"),
            row.get("contact_slot"),
        )

    @staticmethod
    def _join_key(ra_key: object, phone_sanitized: object, contact_slot: object) -> str:
        parts = [
            ResponseReportBuilder._safe_text(ra_key),
            ResponseReportBuilder._safe_text(phone_sanitized),
            ResponseReportBuilder._safe_text(contact_slot),
        ]
        return "|".join(parts)

    @staticmethod
    def _normalize_text(value: object) -> str:
        text = ResponseReportBuilder._safe_text(value).lower()
        text = (
            text.replace("á", "a")
            .replace("à", "a")
            .replace("â", "a")
            .replace("ã", "a")
            .replace("é", "e")
            .replace("ê", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ô", "o")
            .replace("õ", "o")
            .replace("ú", "u")
            .replace("ç", "c")
        )
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _safe_text(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera relatorio final de retornos a partir da campanha e das exportacoes normalizadas do WhatsApp.",
    )
    parser.add_argument(
        "--campaign",
        required=True,
        help="Arquivo da campanha (.xlsx).",
    )
    parser.add_argument(
        "--responses",
        help="Base normalizada de respostas (.xlsx). Se omitido, usa a convencao por campaign_id.",
    )
    parser.add_argument(
        "--output",
        help="Arquivo final .xlsx do relatorio de retornos.",
    )
    args = parser.parse_args()

    builder = ResponseReportBuilder(
        campaign_path=Path(args.campaign),
        responses_path=Path(args.responses) if args.responses else None,
    )
    report_path = builder.run(output_path=Path(args.output) if args.output else None)
    logger.info("Execucao finalizada. Relatorio criado em %s", report_path)


if __name__ == "__main__":
    main()
