import argparse
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


TIMESTAMP_PATTERNS = [
    re.compile(
        r"^(?P<date>\d{1,2}/\d{1,2}/\d{2,4}),\s*(?P<time>\d{1,2}:\d{2}(?::\d{2})?)\s*-\s*(?P<body>.*)$",
    ),
    re.compile(
        r"^\[(?P<date>\d{1,2}/\d{1,2}/\d{2,4}),\s*(?P<time>\d{1,2}:\d{2}(?::\d{2})?)\]\s*(?P<body>.*)$",
    ),
]


@dataclass(frozen=True)
class ParserResult:
    campaign_id: str
    campaign_path: Path
    exports_dir: Path
    output_path: Path
    message_count: int
    matched_file_count: int
    unmatched_file_count: int


class WhatsAppExportParser:
    def __init__(self, campaign_path: Path, exports_dir: Path) -> None:
        self.campaign_path = campaign_path
        self.exports_dir = exports_dir

    def run(self, output_path: Optional[Path] = None) -> ParserResult:
        campaign_df = self._load_campaign(self.campaign_path)
        campaign_id = self._resolve_campaign_id(campaign_df, self.campaign_path)
        source_dir = self._resolve_exports_dir(self.exports_dir, campaign_id)

        if not source_dir.exists():
            raise FileNotFoundError(f"Pasta de exportacoes nao encontrada: {source_dir}")

        export_files = sorted(source_dir.glob("*.txt"))
        if not export_files:
            raise FileNotFoundError(f"Nenhum arquivo .txt encontrado em: {source_dir}")

        logger.info("Processando %s exportacao(oes) em %s", len(export_files), source_dir)

        parsed_frames: list[pd.DataFrame] = []
        matched_files = 0
        unmatched_files = 0
        for file_path in export_files:
            match_info = self._match_export_file(file_path, campaign_df)
            if match_info["matched"]:
                matched_files += 1
            else:
                unmatched_files += 1

            messages_df = self._parse_export_file(file_path, campaign_id, match_info)
            parsed_frames.append(messages_df)

        normalized_df = (
            pd.concat(parsed_frames, ignore_index=True)
            if parsed_frames
            else pd.DataFrame(columns=self._output_columns())
        )
        files_df = pd.DataFrame([self._match_export_file(file_path, campaign_df) for file_path in export_files])

        final_output_path = Path(
            output_path or self.campaign_path.parent / f"WhatsApp_Responses_Normalized_{campaign_id}.xlsx",
        )
        self._write_output(normalized_df, files_df, final_output_path)

        logger.info(
            "Base normalizada gerada em %s | mensagens=%s | arquivos_casados=%s | arquivos_sem_casamento=%s",
            final_output_path,
            len(normalized_df),
            matched_files,
            unmatched_files,
        )

        return ParserResult(
            campaign_id=campaign_id,
            campaign_path=self.campaign_path,
            exports_dir=source_dir,
            output_path=final_output_path,
            message_count=len(normalized_df),
            matched_file_count=matched_files,
            unmatched_file_count=unmatched_files,
        )

    @staticmethod
    def _load_campaign(path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Arquivo de campanha nao encontrado: {path}")
        df = pd.read_excel(path, sheet_name="Campanha")
        required = {"campaign_id", "student_name", "parent_name", "phone_sanitized", "ra_key", "contact_slot"}
        missing = sorted(required.difference(df.columns))
        if missing:
            raise KeyError("Campanha sem colunas obrigatorias: " + ", ".join(missing))
        return df.copy()

    @staticmethod
    def _resolve_campaign_id(campaign_df: pd.DataFrame, campaign_path: Path) -> str:
        if not campaign_df.empty and "campaign_id" in campaign_df.columns:
            campaign_id = str(campaign_df.iloc[0]["campaign_id"]).strip()
            if campaign_id and campaign_id.lower() != "nan":
                return campaign_id
        return campaign_path.stem

    @staticmethod
    def _resolve_exports_dir(exports_dir: Path, campaign_id: str) -> Path:
        candidate = exports_dir / campaign_id if exports_dir.name != campaign_id else exports_dir
        if candidate.exists():
            return candidate
        return exports_dir

    def _match_export_file(self, file_path: Path, campaign_df: pd.DataFrame) -> dict[str, object]:
        source_phone_guess = self._extract_phone_from_name(file_path.stem)
        source_contact_guess = self._normalize_name_guess(file_path.stem)

        match_row = None
        match_method = ""
        if source_phone_guess:
            phone_matches = campaign_df.loc[
                campaign_df["phone_sanitized"].fillna("").astype(str).str.strip().eq(source_phone_guess)
            ]
            if len(phone_matches) == 1:
                match_row = phone_matches.iloc[0]
                match_method = "phone"

        if match_row is None and source_contact_guess:
            parent_norm = campaign_df["parent_name"].fillna("").astype(str).map(self._normalize_text)
            exact_matches = campaign_df.loc[parent_norm.eq(source_contact_guess)]
            if len(exact_matches) == 1:
                match_row = exact_matches.iloc[0]
                match_method = "parent_exact"
            else:
                contains_matches = campaign_df.loc[
                    parent_norm.map(
                        lambda value: bool(value)
                        and (source_contact_guess in value or value in source_contact_guess),
                    )
                ]
                if len(contains_matches) == 1:
                    match_row = contains_matches.iloc[0]
                    match_method = "parent_contains"

        result = {
            "campaign_id": self._resolve_campaign_id(campaign_df, self.campaign_path),
            "source_file": file_path.name,
            "source_file_path": str(file_path),
            "source_phone_guess": source_phone_guess,
            "source_contact_guess": source_contact_guess,
            "matched": bool(match_row is not None),
            "match_method": match_method,
            "matched_ra_key": "",
            "matched_phone": "",
            "matched_parent_name": "",
            "matched_student_name": "",
            "matched_contact_slot": "",
        }
        if match_row is not None:
            result.update(
                {
                    "matched_ra_key": self._safe_text(match_row.get("ra_key")),
                    "matched_phone": self._safe_text(match_row.get("phone_sanitized")),
                    "matched_parent_name": self._safe_text(match_row.get("parent_name")),
                    "matched_student_name": self._safe_text(match_row.get("student_name")),
                    "matched_contact_slot": self._safe_text(match_row.get("contact_slot")),
                }
            )
        return result

    def _parse_export_file(
        self,
        file_path: Path,
        campaign_id: str,
        match_info: dict[str, object],
    ) -> pd.DataFrame:
        text = self._read_text_file(file_path)
        lines = text.splitlines()

        entries: list[dict[str, object]] = []
        current_entry: Optional[dict[str, object]] = None
        for raw_line in lines:
            parsed = self._parse_message_line(raw_line)
            if parsed is None:
                if current_entry is not None:
                    continuation = raw_line.strip()
                    if continuation:
                        current_entry["message_text"] = (
                            f"{current_entry['message_text']}\n{continuation}".strip()
                        )
                continue

            if current_entry is not None:
                entries.append(current_entry)
            current_entry = {
                "campaign_id": campaign_id,
                "source_file": file_path.name,
                "source_file_path": str(file_path),
                "source_phone_guess": match_info["source_phone_guess"],
                "source_contact_guess": match_info["source_contact_guess"],
                "matched": match_info["matched"],
                "match_method": match_info["match_method"],
                "matched_ra_key": match_info["matched_ra_key"],
                "matched_phone": match_info["matched_phone"],
                "matched_parent_name": match_info["matched_parent_name"],
                "matched_student_name": match_info["matched_student_name"],
                "matched_contact_slot": match_info["matched_contact_slot"],
                "message_datetime": parsed["message_datetime"],
                "message_date": parsed["message_date"],
                "message_time": parsed["message_time"],
                "author_label": parsed["author_label"],
                "message_text": parsed["message_text"],
            }

        if current_entry is not None:
            entries.append(current_entry)

        if not entries:
            return pd.DataFrame(columns=self._output_columns())
        return pd.DataFrame(entries)[self._output_columns()]

    def _parse_message_line(self, line: str) -> Optional[dict[str, str]]:
        for pattern in TIMESTAMP_PATTERNS:
            match = pattern.match(line.strip())
            if not match:
                continue

            date_text = match.group("date").strip()
            time_text = match.group("time").strip()
            body = match.group("body").strip()
            message_datetime = self._parse_datetime(date_text, time_text)

            author_label = ""
            message_text = body
            if ": " in body:
                author_label, message_text = body.split(": ", 1)
            return {
                "message_datetime": message_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "message_date": message_datetime.strftime("%Y-%m-%d"),
                "message_time": message_datetime.strftime("%H:%M:%S"),
                "author_label": author_label.strip(),
                "message_text": message_text.strip(),
            }
        return None

    @staticmethod
    def _parse_datetime(date_text: str, time_text: str):
        normalized_year = date_text.split("/")[-1]
        normalized_time = time_text if len(time_text.split(":")) == 3 else f"{time_text}:00"
        date_candidate = date_text
        if len(normalized_year) == 2:
            date_candidate = re.sub(r"/(\d{2})$", r"/20\1", date_text)
        return pd.to_datetime(
            f"{date_candidate} {normalized_time}",
            dayfirst=True,
            errors="raise",
        ).to_pydatetime()

    @staticmethod
    def _read_text_file(path: Path) -> str:
        encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
        for encoding in encodings:
            try:
                return path.read_text(encoding=encoding)
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError("unknown", b"", 0, 1, f"Nao foi possivel ler {path.name}")

    @staticmethod
    def _extract_phone_from_name(file_stem: str) -> str:
        digits = re.sub(r"\D", "", file_stem)
        if len(digits) >= 12:
            return digits[-13:] if len(digits) >= 13 else digits
        return ""

    @staticmethod
    def _normalize_name_guess(file_stem: str) -> str:
        text = file_stem
        replacements = [
            "whatsapp chat with",
            "conversa do whatsapp com",
            "chat do whatsapp com",
            "chat with",
        ]
        normalized = WhatsAppExportParser._normalize_text(text)
        for token in replacements:
            normalized = normalized.replace(token, "").strip()
        return normalized

    @staticmethod
    def _normalize_text(value: object) -> str:
        text = WhatsAppExportParser._safe_text(value).lower()
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

    @staticmethod
    def _output_columns() -> list[str]:
        return [
            "campaign_id",
            "source_file",
            "source_file_path",
            "source_phone_guess",
            "source_contact_guess",
            "matched",
            "match_method",
            "matched_ra_key",
            "matched_phone",
            "matched_parent_name",
            "matched_student_name",
            "matched_contact_slot",
            "message_datetime",
            "message_date",
            "message_time",
            "author_label",
            "message_text",
        ]

    @staticmethod
    def _write_output(messages_df: pd.DataFrame, files_df: pd.DataFrame, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            messages_df.to_excel(writer, sheet_name="Messages", index=False)
            files_df.to_excel(writer, sheet_name="Files", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parser de exportacoes .txt do WhatsApp para uma campanha especifica.",
    )
    parser.add_argument(
        "--campaign",
        required=True,
        help="Arquivo da campanha (.xlsx) que servira de referencia para casar os contatos.",
    )
    parser.add_argument(
        "--exports-dir",
        default="exports_whatsapp",
        help="Pasta base das exportacoes do WhatsApp. Padrao: exports_whatsapp",
    )
    parser.add_argument(
        "--output",
        help="Arquivo de saida .xlsx da base normalizada.",
    )
    args = parser.parse_args()

    campaign_path = Path(args.campaign)
    exports_dir = Path(args.exports_dir)
    output_path = Path(args.output) if args.output else None

    runner = WhatsAppExportParser(campaign_path=campaign_path, exports_dir=exports_dir)
    result = runner.run(output_path=output_path)
    logger.info(
        "Parser finalizado | campaign_id=%s | mensagens=%s | arquivos_casados=%s | arquivos_sem_casamento=%s",
        result.campaign_id,
        result.message_count,
        result.matched_file_count,
        result.unmatched_file_count,
    )


if __name__ == "__main__":
    main()
