import argparse
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConversationMessage:
    conversation_id: str
    conversation_header: str
    phone_guess: str
    message_dt: pd.Timestamp
    author: str
    text: str


class DailyRawReportBuilder:
    def __init__(self, ledger_path: Path, raw_txt_path: Path, campaign_id: str) -> None:
        self.ledger_path = ledger_path
        self.raw_txt_path = raw_txt_path
        self.campaign_id = campaign_id

    def run(self, output_path: Path) -> Path:
        campaign_df = self._load_campaign()
        messages = self._parse_raw_text()
        conv_map = self._match_conversations_to_contacts(messages, campaign_df)
        report_df, incoming_df = self._build_contact_status(campaign_df, messages, conv_map)
        motives_df = self._extract_motives(incoming_df)

        summary_df = self._build_summary(report_df)
        motives_summary_df = (
            motives_df.groupby("motivo_categoria").size().reset_index(name="quantidade")
            if not motives_df.empty
            else pd.DataFrame(columns=["motivo_categoria", "quantidade"])
        )
        motives_summary_df = motives_summary_df.sort_values("quantidade", ascending=False)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            summary_df.to_excel(writer, sheet_name="Resumo", index=False)
            report_df.to_excel(writer, sheet_name="Contatos_Detalhados", index=False)
            incoming_df.to_excel(writer, sheet_name="Respostas_Brutas", index=False)
            motives_summary_df.to_excel(writer, sheet_name="Motivos_Consolidados", index=False)
            motives_df.to_excel(writer, sheet_name="Motivos_Detalhados", index=False)
        return output_path

    def _load_campaign(self) -> pd.DataFrame:
        df = pd.read_excel(self.ledger_path, sheet_name="Historico")
        df = df[df["campaign_id"].fillna("").astype(str).eq(self.campaign_id)].copy()
        if df.empty:
            raise ValueError(f"Nenhum registro encontrado para campaign_id={self.campaign_id}")

        for col in ["ra_key", "contact_slot", "student_name", "parent_name", "status_envio", "status_resposta", "observacao"]:
            df[col] = df[col].fillna("").astype(str)
        df["phone_sanitized"] = (
            df["phone_sanitized"]
            .fillna("")
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\D", "", regex=True)
        )
        df["data_envio_dt"] = pd.to_datetime(df["data_envio"], errors="coerce")
        df["status_envio"] = df["status_envio"].str.lower().str.strip()
        df["status_resposta"] = df["status_resposta"].str.lower().str.strip()
        df["contact_key"] = df.apply(
            lambda r: f"{r['ra_key']}|{r['phone_sanitized']}|{r['contact_slot']}",
            axis=1,
        )
        # Remove linhas de legenda/manual sem aluno/RA
        df = df[df["student_name"].str.strip().ne("") & df["ra_key"].str.strip().ne("")].copy()
        # Mantem ultimo evento por contato
        df = df.sort_values(["data_envio_dt", "data_envio"]).drop_duplicates("contact_key", keep="last")
        return df

    def _parse_raw_text(self) -> list[ConversationMessage]:
        text = self.raw_txt_path.read_text(encoding="utf-8", errors="ignore")
        parts = re.split(r"\s*=+\s*Conversa do WhatsApp com\s+(.+?)\s*=+\s*", text)
        line_re = re.compile(r"^(\d{1,2}/\d{1,2}/\d{2,4})\s+(\d{1,2}:\d{2})\s*-\s*(.+?)\s*:\s*(.*)$")

        messages: list[ConversationMessage] = []
        for i in range(1, len(parts), 2):
            header = parts[i].strip()
            body = parts[i + 1]
            conversation_id = f"conv_{i//2+1:04d}"
            phone_guess = self._extract_phone(header)
            for raw_line in body.replace("\r", "\n").split("\n"):
                line = raw_line.strip()
                if not line:
                    continue
                m = line_re.match(line)
                if not m:
                    continue
                d, hhmm, author, msg = m.groups()
                dt = pd.to_datetime(f"{d} {hhmm}:00", dayfirst=True, errors="coerce")
                if pd.isna(dt):
                    continue
                messages.append(
                    ConversationMessage(
                        conversation_id=conversation_id,
                        conversation_header=header,
                        phone_guess=phone_guess,
                        message_dt=dt,
                        author=author.strip(),
                        text=msg.strip(),
                    )
                )
        return messages

    def _match_conversations_to_contacts(
        self,
        messages: list[ConversationMessage],
        campaign_df: pd.DataFrame,
    ) -> dict[str, str]:
        conv_to_key: dict[str, str] = {}
        by_conv: dict[str, list[ConversationMessage]] = {}
        for msg in messages:
            by_conv.setdefault(msg.conversation_id, []).append(msg)

        # Indexes
        phone_to_key = {
            r["phone_sanitized"]: r["contact_key"]
            for _, r in campaign_df.iterrows()
            if r["phone_sanitized"]
        }
        student_norm_rows = [
            (self._norm(r["student_name"]), r["contact_key"])
            for _, r in campaign_df.iterrows()
            if r["student_name"]
        ]
        parent_norm_rows = [
            (self._norm(r["parent_name"]), r["contact_key"])
            for _, r in campaign_df.iterrows()
            if r["parent_name"]
        ]

        for conv_id, rows in by_conv.items():
            header = rows[0].conversation_header
            phone_guess = rows[0].phone_guess
            matched_key = ""

            # 1) Phone match
            if phone_guess and phone_guess in phone_to_key:
                matched_key = phone_to_key[phone_guess]

            # 2) Student name in school message
            if not matched_key:
                school_lines = " ".join(
                    self._norm(r.text)
                    for r in rows
                    if "escola" in self._norm(r.author)
                )
                hits = [key for name, key in student_norm_rows if name and name in school_lines]
                hits = list(dict.fromkeys(hits))
                if len(hits) == 1:
                    matched_key = hits[0]

            # 3) Header by parent name
            if not matched_key:
                h = self._norm(header)
                hits = [key for name, key in parent_norm_rows if name and (name in h or h in name)]
                hits = list(dict.fromkeys(hits))
                if len(hits) == 1:
                    matched_key = hits[0]

            if matched_key:
                conv_to_key[conv_id] = matched_key

        return conv_to_key

    def _build_contact_status(
        self,
        campaign_df: pd.DataFrame,
        messages: list[ConversationMessage],
        conv_map: dict[str, str],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        incoming_rows: list[dict[str, object]] = []
        for msg in messages:
            if msg.conversation_id not in conv_map:
                continue
            contact_key = conv_map[msg.conversation_id]
            if "escola" in self._norm(msg.author):
                continue
            if "mensagens e ligacoes sao protegidas" in self._norm(msg.text):
                continue
            incoming_rows.append(
                {
                    "contact_key": contact_key,
                    "conversation_id": msg.conversation_id,
                    "conversation_header": msg.conversation_header,
                    "message_datetime": msg.message_dt,
                    "author": msg.author,
                    "text": msg.text,
                }
            )
        incoming_df = pd.DataFrame(incoming_rows)

        report = campaign_df.copy()
        report["respondeu_txt"] = False
        report["qtd_respostas_txt"] = 0
        report["primeira_resposta_datahora"] = ""
        report["primeira_resposta_autor"] = ""
        report["primeira_resposta_texto"] = ""

        if not incoming_df.empty:
            for idx, row in report.iterrows():
                subset = incoming_df[incoming_df["contact_key"].eq(row["contact_key"])].copy()
                if pd.notna(row["data_envio_dt"]):
                    subset = subset[subset["message_datetime"] >= row["data_envio_dt"]]
                subset = subset.sort_values("message_datetime")
                if subset.empty:
                    continue
                first = subset.iloc[0]
                report.at[idx, "respondeu_txt"] = True
                report.at[idx, "qtd_respostas_txt"] = int(len(subset))
                report.at[idx, "primeira_resposta_datahora"] = first["message_datetime"].strftime(
                    "%Y-%m-%d %H:%M:%S",
                )
                report.at[idx, "primeira_resposta_autor"] = first["author"]
                report.at[idx, "primeira_resposta_texto"] = first["text"]

        # "respondeu_final" respeita anotacao manual no ledger
        report["respondeu_ledger"] = report["status_resposta"].eq("respondido")
        report["respondeu_final"] = report["respondeu_txt"] | report["respondeu_ledger"]

        export_cols = [
            "class_name",
            "student_name",
            "parent_name",
            "phone_sanitized",
            "status_envio",
            "data_envio",
            "status_resposta",
            "respondeu_ledger",
            "respondeu_txt",
            "respondeu_final",
            "qtd_respostas_txt",
            "primeira_resposta_datahora",
            "primeira_resposta_autor",
            "primeira_resposta_texto",
            "observacao",
        ]
        return report[export_cols], incoming_df

    def _extract_motives(self, incoming_df: pd.DataFrame) -> pd.DataFrame:
        if incoming_df.empty:
            return pd.DataFrame(columns=["contact_key", "author", "motivo_texto", "motivo_categoria", "text"])

        rows: list[dict[str, str]] = []
        for _, r in incoming_df.iterrows():
            txt = str(r["text"])
            notes = re.findall(r"<([^>]+)>", txt)
            if notes:
                for note in notes:
                    rows.append(
                        {
                            "contact_key": r["contact_key"],
                            "author": str(r["author"]),
                            "motivo_texto": note.strip(),
                            "motivo_categoria": self._classify_motive(note),
                            "text": txt,
                        }
                    )
            else:
                rows.append(
                    {
                        "contact_key": r["contact_key"],
                        "author": str(r["author"]),
                        "motivo_texto": "",
                        "motivo_categoria": self._classify_motive(txt),
                        "text": txt,
                    }
                )
        return pd.DataFrame(rows)

    def _build_summary(self, report_df: pd.DataFrame) -> pd.DataFrame:
        total = len(report_df)
        enviados = int(report_df["status_envio"].eq("enviado").sum())
        falhas = int(report_df["status_envio"].eq("falha").sum())
        pendentes = int(report_df["status_envio"].isin(["", "pendente"]).sum())
        inval = int(report_df["status_resposta"].eq("numero_invalido").sum())
        resp_txt = int(report_df["respondeu_txt"].sum())
        resp_ledger = int(report_df["respondeu_ledger"].sum())
        resp_final = int(report_df["respondeu_final"].sum())
        taxa = round((resp_final / enviados) * 100, 2) if enviados else 0.0
        return pd.DataFrame(
            [
                {"indicador": "campaign_id", "valor": self.campaign_id},
                {"indicador": "total_contatos_unicos", "valor": total},
                {"indicador": "total_enviados", "valor": enviados},
                {"indicador": "total_falhas", "valor": falhas},
                {"indicador": "total_pendentes", "valor": pendentes},
                {"indicador": "total_numero_invalido", "valor": inval},
                {"indicador": "respondidos_no_txt", "valor": resp_txt},
                {"indicador": "respondidos_no_ledger", "valor": resp_ledger},
                {"indicador": "respondidos_finais", "valor": resp_final},
                {"indicador": "taxa_resposta_final_percentual", "valor": taxa},
            ]
        )

    @staticmethod
    def _extract_phone(text: str) -> str:
        digits = re.sub(r"\D", "", text or "")
        if len(digits) >= 13:
            return digits[-13:]
        if len(digits) == 12:
            return digits
        return ""

    @staticmethod
    def _norm(value: str) -> str:
        text = (value or "").lower()
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
            .replace("‎", "")
        )
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _classify_motive(text: str) -> str:
        t = DailyRawReportBuilder._norm(text)
        if any(k in t for k in ["saude", "medic", "hospital", "oculista", "consulta", "doente", "atestado", "oftalmo"]):
            return "saude"
        if any(k in t for k in ["trabalho", "servico", "emprego"]):
            return "trabalho"
        if any(k in t for k in ["onibus", "transporte", "conducao"]):
            return "transporte"
        if any(k in t for k in ["mudou", "mudanca", "endereco"]):
            return "mudanca"
        if any(k in t for k in ["mae", "pai", "avo", "famil"]):
            return "familia"
        if not t:
            return "sem_classificacao"
        return "outros"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera relatorio analitico diario cruzando Daily_Campaign_Ledger com TXT bruto de conversas.",
    )
    parser.add_argument(
        "--ledger",
        default="relatorios/Daily_Campaign_Ledger.xlsx",
        help="Arquivo do Daily Campaign Ledger.",
    )
    parser.add_argument(
        "--raw-txt",
        default="relatorios/TXT_BRUTO_TOTAL_ULTIMOS_ZIPS.txt",
        help="Arquivo TXT bruto consolidado das conversas exportadas.",
    )
    parser.add_argument(
        "--campaign-id",
        required=True,
        help="Campaign ID para filtrar (ex.: Campanha_Diaria_2026_03_20_dia_19).",
    )
    parser.add_argument(
        "--output",
        default="relatorios/Relatorio_Analitico_Dia19_Realizado_Dia20.xlsx",
        help="Arquivo de saida do relatorio.",
    )
    args = parser.parse_args()

    builder = DailyRawReportBuilder(
        ledger_path=Path(args.ledger),
        raw_txt_path=Path(args.raw_txt),
        campaign_id=args.campaign_id,
    )
    output_path = builder.run(output_path=Path(args.output))
    logger.info("Relatorio gerado em %s", output_path)


if __name__ == "__main__":
    main()
