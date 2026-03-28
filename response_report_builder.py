import argparse
import logging
from pathlib import Path

import pandas as pd

from config import get_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_DAILY_LEDGER_PATH = Path("relatorios/Daily_Campaign_Ledger.xlsx")
NON_RESPONSE_STATUSES = {"", "sem_resposta", "numero_invalido", "pendente"}


class ResponseReportBuilder:
    def __init__(self, campaign_path: Path, responses_path: Path | None = None) -> None:
        self.campaign_path = campaign_path
        self.responses_path = responses_path
        self.settings = get_settings()

    def run(self, output_path: Path | None = None) -> Path:
        campaign_df = self._load_campaign()
        campaign_id = self._resolve_campaign_id(campaign_df)
        responses_path = self._resolve_responses_path(campaign_id)
        matches_df, messages_df, files_df = self._load_responses(responses_path)
        ledger_df = self._load_ledger()

        enriched_campaign_df = self._prepare_campaign(campaign_df, ledger_df)
        prepared_matches_df = self._prepare_matches(matches_df)
        report_parts = self._build_report_parts(enriched_campaign_df, prepared_matches_df, messages_df, files_df)

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

    def _load_responses(self, path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if not path.exists():
            raise FileNotFoundError(f"Base normalizada de respostas nao encontrada: {path}")

        excel_file = pd.ExcelFile(path)
        sheet_names = set(excel_file.sheet_names)
        matches_df = pd.read_excel(path, sheet_name="Matches") if "Matches" in sheet_names else pd.DataFrame()
        messages_df = pd.read_excel(path, sheet_name="Messages") if "Messages" in sheet_names else pd.DataFrame()
        files_df = pd.read_excel(path, sheet_name="Files") if "Files" in sheet_names else pd.DataFrame()
        return matches_df, messages_df, files_df

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
        for column in [
            "campaign_id",
            "class_name",
            "student_name",
            "parent_name",
            "phone_sanitized",
            "ra_key",
            "contact_slot",
            "status_envio",
            "status_resposta",
            "observacao",
            "whatsapp_message",
        ]:
            if column in prepared.columns:
                prepared[column] = prepared[column].apply(self._safe_text)

        prepared["phone_sanitized"] = prepared["phone_sanitized"].map(self._digits_only)
        prepared["data_envio_dt"] = pd.to_datetime(prepared.get("data_envio"), errors="coerce")
        prepared["campaign_key"] = prepared.apply(self._build_campaign_key, axis=1)
        prepared["status_resposta_norm"] = prepared["status_resposta"].map(self._normalize_status)

        if ledger_df.empty:
            prepared["ledger_status_resposta"] = ""
            prepared["ledger_observacao"] = ""
            return prepared

        ledger_prepared = ledger_df.copy()
        for column in ["ra_key", "phone_sanitized", "contact_slot", "status_resposta", "observacao"]:
            if column in ledger_prepared.columns:
                ledger_prepared[column] = ledger_prepared[column].apply(self._safe_text)
        ledger_prepared["phone_sanitized"] = ledger_prepared["phone_sanitized"].map(self._digits_only)
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

    def _prepare_matches(self, matches_df: pd.DataFrame) -> pd.DataFrame:
        if matches_df.empty:
            return pd.DataFrame(columns=["campaign_key"])

        prepared = matches_df.copy()
        for column in [
            "campaign_id",
            "campaign_key",
            "source_file",
            "source_file_path",
            "conversation_id",
            "conversation_header",
            "match_method",
            "matched_student_name",
            "matched_parent_name",
            "matched_phone",
            "matched_contact_slot",
            "matched_ra_key",
            "campaign_prompt_text",
            "campaign_prompt_author",
            "first_reply_author",
            "first_reply_text",
            "reason_category_suggested",
            "reason_text_excerpt",
            "review_reason",
        ]:
            if column in prepared.columns:
                prepared[column] = prepared[column].apply(self._safe_text)

        prepared["matched_phone"] = prepared["matched_phone"].map(self._digits_only)
        prepared["first_reply_after_send_dt"] = pd.to_datetime(
            prepared.get("first_reply_after_send_datetime"),
            errors="coerce",
        )
        prepared["campaign_prompt_dt"] = pd.to_datetime(prepared.get("campaign_prompt_datetime"), errors="coerce")
        prepared["has_campaign_prompt"] = prepared.get("has_campaign_prompt", False).fillna(False).astype(bool)
        prepared["needs_review"] = prepared.get("needs_review", False).fillna(False).astype(bool)
        prepared["respondeu_por_parser"] = prepared["first_reply_after_send_dt"].notna()
        return prepared

    def _build_report_parts(
        self,
        campaign_df: pd.DataFrame,
        matches_df: pd.DataFrame,
        messages_df: pd.DataFrame,
        files_df: pd.DataFrame,
    ) -> dict[str, pd.DataFrame]:
        matches_summary = self._summarize_matches(matches_df)
        report_df = campaign_df.merge(matches_summary, on="campaign_key", how="left")
        report_df = report_df.fillna(
            {
                "match_method": "",
                "source_file": "",
                "source_file_path": "",
                "conversation_id": "",
                "campaign_prompt_datetime": "",
                "first_reply_after_send_datetime": "",
                "first_reply_author": "",
                "first_reply_text": "",
                "reason_category_suggested": "",
                "reason_text_excerpt": "",
                "review_reason": "",
                "respondeu_por_parser": False,
                "has_campaign_prompt": False,
                "needs_review": False,
                "reply_count_after_send": 0,
            }
        )

        report_df["ledger_status_resposta"] = report_df.get("ledger_status_resposta", "").fillna("")
        report_df["ledger_observacao"] = report_df.get("ledger_observacao", "").fillna("")
        report_df["respondeu_por_ledger"] = report_df.apply(self._detect_ledger_response, axis=1)
        report_df["respondeu_final"] = report_df["respondeu_por_parser"] | report_df["respondeu_por_ledger"]
        report_df["status_numero_invalido"] = report_df["status_resposta_norm"].eq("numero_invalido")
        report_df["review_reason"] = report_df["review_reason"].fillna("").astype(str)
        report_df.loc[
            report_df["respondeu_por_parser"] != report_df["respondeu_por_ledger"],
            "review_reason",
        ] = report_df.loc[
            report_df["respondeu_por_parser"] != report_df["respondeu_por_ledger"],
            "review_reason",
        ].map(lambda value: self._append_reason(value, "divergencia_parser_ledger"))
        report_df.loc[
            report_df["status_envio"].str.lower().eq("enviado") & report_df["match_method"].fillna("").eq(""),
            "review_reason",
        ] = report_df.loc[
            report_df["status_envio"].str.lower().eq("enviado") & report_df["match_method"].fillna("").eq(""),
            "review_reason",
        ].map(lambda value: self._append_reason(value, "sem_match_de_conversa"))
        report_df["revisar_final"] = report_df["needs_review"] | (
            report_df["respondeu_por_parser"] != report_df["respondeu_por_ledger"]
        ) | report_df["match_method"].fillna("").eq("")

        common_columns = [
            "campaign_id",
            "class_name",
            "student_name",
            "parent_name",
            "phone_sanitized",
            "data_envio",
            "status_envio",
            "status_resposta",
            "ledger_status_resposta",
            "respondeu_por_parser",
            "respondeu_por_ledger",
            "respondeu_final",
            "match_method",
            "source_file",
            "source_file_path",
            "campaign_prompt_datetime",
            "first_reply_after_send_datetime",
            "first_reply_author",
            "first_reply_text",
            "reason_category_suggested",
            "reason_text_excerpt",
            "review_reason",
            "observacao",
            "ledger_observacao",
        ]

        responded_df = report_df.loc[report_df["respondeu_final"]].copy()[common_columns]
        sem_retorno_df = report_df.loc[
            report_df["status_envio"].str.lower().eq("enviado")
            & ~report_df["respondeu_final"]
            & ~report_df["status_numero_invalido"]
        ].copy()[common_columns]

        nao_recontatar_df = report_df.loc[
            report_df["respondeu_final"] | report_df["status_numero_invalido"]
        ].copy()[
            [
                "campaign_id",
                "class_name",
                "student_name",
                "parent_name",
                "phone_sanitized",
                "data_envio",
                "status_envio",
                "status_resposta",
                "respondeu_final",
                "reason_category_suggested",
                "source_file_path",
            ]
        ]

        justificativas_df = report_df.loc[report_df["first_reply_text"].fillna("").astype(str).str.strip().ne("")].copy()[
            [
                "campaign_id",
                "class_name",
                "student_name",
                "parent_name",
                "phone_sanitized",
                "data_envio",
                "first_reply_after_send_datetime",
                "first_reply_author",
                "first_reply_text",
                "reason_category_suggested",
                "reason_text_excerpt",
                "source_file",
                "source_file_path",
                "review_reason",
            ]
        ]

        revisar_df = report_df.loc[report_df["revisar_final"]].copy()[
            [
                "campaign_id",
                "class_name",
                "student_name",
                "parent_name",
                "phone_sanitized",
                "data_envio",
                "status_envio",
                "status_resposta",
                "ledger_status_resposta",
                "respondeu_por_parser",
                "respondeu_por_ledger",
                "respondeu_final",
                "match_method",
                "source_file",
                "source_file_path",
                "campaign_prompt_datetime",
                "first_reply_after_send_datetime",
                "first_reply_text",
                "reason_category_suggested",
                "review_reason",
                "observacao",
                "ledger_observacao",
            ]
        ]

        total_enviados = int(report_df["status_envio"].str.lower().eq("enviado").sum())
        total_respondidos_parser = int(report_df["respondeu_por_parser"].sum())
        total_respondidos_ledger = int(report_df["respondeu_por_ledger"].sum())
        total_respondidos_final = int(report_df["respondeu_final"].sum())
        total_sem_retorno = len(sem_retorno_df)
        total_numero_invalido = int(report_df["status_numero_invalido"].sum())
        total_revisar = len(revisar_df)
        taxa_resposta = round((total_respondidos_final / total_enviados) * 100, 2) if total_enviados else 0.0

        resumo_df = pd.DataFrame(
            [
                {"indicador": "campaign_id", "valor": self._resolve_campaign_id(campaign_df)},
                {"indicador": "total_registros_campanha", "valor": len(report_df)},
                {"indicador": "total_enviados", "valor": total_enviados},
                {"indicador": "total_respondidos_parser", "valor": total_respondidos_parser},
                {"indicador": "total_respondidos_ledger", "valor": total_respondidos_ledger},
                {"indicador": "total_respondidos_final", "valor": total_respondidos_final},
                {"indicador": "total_sem_retorno", "valor": total_sem_retorno},
                {"indicador": "total_numero_invalido", "valor": total_numero_invalido},
                {"indicador": "total_para_revisar", "valor": total_revisar},
                {"indicador": "taxa_resposta_percentual", "valor": taxa_resposta},
                {"indicador": "total_arquivos_importados", "valor": len(files_df) if not files_df.empty else 0},
                {"indicador": "total_mensagens_importadas", "valor": len(messages_df) if not messages_df.empty else 0},
            ]
        )

        return {
            "Resumo": resumo_df,
            "Respondidos": responded_df,
            "Sem_Retorno": sem_retorno_df,
            "Nao_Recontatar": nao_recontatar_df,
            "Justificativas": justificativas_df,
            "Revisar": revisar_df,
        }

    def _summarize_matches(self, matches_df: pd.DataFrame) -> pd.DataFrame:
        if matches_df.empty:
            return pd.DataFrame(
                columns=[
                    "campaign_key",
                    "match_method",
                    "source_file",
                    "source_file_path",
                    "conversation_id",
                    "campaign_prompt_datetime",
                    "first_reply_after_send_datetime",
                    "first_reply_author",
                    "first_reply_text",
                    "reason_category_suggested",
                    "reason_text_excerpt",
                    "review_reason",
                    "respondeu_por_parser",
                    "has_campaign_prompt",
                    "needs_review",
                    "reply_count_after_send",
                ]
            )

        summary_rows: list[dict[str, object]] = []
        for campaign_key, group_df in matches_df.groupby("campaign_key", dropna=False):
            ordered = group_df.sort_values(
                ["respondeu_por_parser", "campaign_prompt_dt", "first_reply_after_send_dt"],
                ascending=[False, True, True],
                na_position="last",
            )
            best = ordered.iloc[0]
            review_reasons = " | ".join(
                sorted({self._safe_text(value) for value in group_df["review_reason"] if self._safe_text(value)})
            )
            summary_rows.append(
                {
                    "campaign_key": campaign_key,
                    "match_method": self._safe_text(best.get("match_method")),
                    "source_file": self._safe_text(best.get("source_file")),
                    "source_file_path": self._safe_text(best.get("source_file_path")),
                    "conversation_id": self._safe_text(best.get("conversation_id")),
                    "campaign_prompt_datetime": self._safe_text(best.get("campaign_prompt_datetime")),
                    "first_reply_after_send_datetime": self._safe_text(best.get("first_reply_after_send_datetime")),
                    "first_reply_author": self._safe_text(best.get("first_reply_author")),
                    "first_reply_text": self._safe_text(best.get("first_reply_text")),
                    "reason_category_suggested": self._safe_text(best.get("reason_category_suggested")),
                    "reason_text_excerpt": self._safe_text(best.get("reason_text_excerpt")),
                    "review_reason": review_reasons,
                    "respondeu_por_parser": bool(group_df["respondeu_por_parser"].any()),
                    "has_campaign_prompt": bool(group_df["has_campaign_prompt"].any()),
                    "needs_review": bool(group_df["needs_review"].any() or len(group_df) > 1),
                    "reply_count_after_send": int(group_df["reply_count_after_send"].fillna(0).max()),
                }
            )
        return pd.DataFrame(summary_rows)

    def _detect_ledger_response(self, row: pd.Series) -> bool:
        campaign_status = self._normalize_status(row.get("status_resposta"))
        ledger_status = self._normalize_status(row.get("ledger_status_resposta"))
        return campaign_status not in NON_RESPONSE_STATUSES or ledger_status not in NON_RESPONSE_STATUSES

    def _write_report(self, report_parts: dict[str, pd.DataFrame], report_path: Path) -> None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
            for sheet_name, df in report_parts.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    @staticmethod
    def _safe_text(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        return str(value).strip()

    @staticmethod
    def _digits_only(value: object) -> str:
        return "".join(character for character in ResponseReportBuilder._safe_text(value) if character.isdigit())

    @staticmethod
    def _normalize_status(value: object) -> str:
        return ResponseReportBuilder._safe_text(value).strip().lower()

    @staticmethod
    def _build_campaign_key(row: pd.Series) -> str:
        return "|".join(
            [
                ResponseReportBuilder._safe_text(row.get("ra_key")),
                ResponseReportBuilder._digits_only(row.get("phone_sanitized")),
                ResponseReportBuilder._safe_text(row.get("contact_slot")),
            ]
        )

    @staticmethod
    def _append_reason(current: str, new_reason: str) -> str:
        parts = [part.strip() for part in ResponseReportBuilder._safe_text(current).split("|") if part.strip()]
        if new_reason and new_reason not in parts:
            parts.append(new_reason)
        return " | ".join(parts)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera relatorio consolidado de retornos do WhatsApp para uma campanha.",
    )
    parser.add_argument(
        "--campaign",
        required=True,
        help="Caminho do arquivo Excel da campanha.",
    )
    parser.add_argument(
        "--responses",
        help="Caminho opcional da base normalizada gerada pelo parser.",
    )
    parser.add_argument(
        "--output",
        help="Caminho opcional do relatorio final.",
    )
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    builder = ResponseReportBuilder(
        campaign_path=Path(args.campaign),
        responses_path=Path(args.responses) if args.responses else None,
    )
    report_path = builder.run(output_path=Path(args.output) if args.output else None)
    logger.info("Execucao finalizada. Relatorio criado em %s", report_path)


if __name__ == "__main__":
    main()
