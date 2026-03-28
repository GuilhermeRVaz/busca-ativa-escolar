import argparse
import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
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
        r"^(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s+(?P<time>\d{1,2}:\d{2}(?::\d{2})?)\s*-\s*(?P<body>.*)$",
    ),
    re.compile(
        r"^\[(?P<date>\d{1,2}/\d{1,2}/\d{2,4}),\s*(?P<time>\d{1,2}:\d{2}(?::\d{2})?)\]\s*(?P<body>.*)$",
    ),
]

DATETIME_FORMATS = [
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%y %H:%M:%S",
    "%d/%m/%y %H:%M",
]

SYSTEM_MESSAGE_MARKERS = (
    "as mensagens e ligacoes sao protegidas",
    "as mensagens e chamadas sao protegidas",
    "saiba mais",
)

GENERIC_PARENT_LABELS = {
    "mae",
    "pai",
    "mamae",
    "papai",
    "responsavel",
    "avo",
    "tia",
    "tio",
}


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
        self.taxonomy = self._load_reason_taxonomy()

    def run(self, output_path: Optional[Path] = None) -> ParserResult:
        campaign_df = self._prepare_campaign_df(self._load_campaign(self.campaign_path))
        campaign_id = self._resolve_campaign_id(campaign_df, self.campaign_path)
        source_dir = self._resolve_exports_dir(self.exports_dir, campaign_id)

        if not source_dir.exists():
            raise FileNotFoundError(f"Pasta de exportacoes nao encontrada: {source_dir}")

        export_files = sorted(source_dir.glob("*.txt"))
        if not export_files:
            raise FileNotFoundError(f"Nenhum arquivo .txt encontrado em: {source_dir}")

        logger.info("Processando %s exportacao(oes) em %s", len(export_files), source_dir)

        file_records: list[dict[str, object]] = []
        message_frames: list[pd.DataFrame] = []
        conversations: list[dict[str, object]] = []

        for file_path in export_files:
            conversation = self._parse_export_file(file_path, campaign_id)
            conversations.append(conversation)
            message_frames.append(conversation["messages_df"])
            file_records.append(conversation["file_record"])

        messages_df = (
            pd.concat(message_frames, ignore_index=True)
            if message_frames
            else pd.DataFrame(columns=self._message_columns())
        )
        matches_df = self._build_matches(campaign_df, conversations)
        files_df = self._build_files_df(file_records, matches_df)

        final_output_path = Path(
            output_path or self.campaign_path.parent / f"WhatsApp_Responses_Normalized_{campaign_id}.xlsx",
        )
        self._write_output(messages_df, files_df, matches_df, final_output_path)

        file_match_summary = files_df["matched_contact_count"].fillna(0).astype(int)
        matched_files = int((file_match_summary > 0).sum())
        unmatched_files = int((file_match_summary == 0).sum())

        logger.info(
            "Base normalizada gerada em %s | mensagens=%s | arquivos_casados=%s | arquivos_sem_casamento=%s",
            final_output_path,
            len(messages_df),
            matched_files,
            unmatched_files,
        )

        return ParserResult(
            campaign_id=campaign_id,
            campaign_path=self.campaign_path,
            exports_dir=source_dir,
            output_path=final_output_path,
            message_count=len(messages_df),
            matched_file_count=matched_files,
            unmatched_file_count=unmatched_files,
        )

    @staticmethod
    def _load_campaign(path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Arquivo de campanha nao encontrado: {path}")
        df = pd.read_excel(path, sheet_name="Campanha")
        required = {
            "campaign_id",
            "student_name",
            "parent_name",
            "phone_sanitized",
            "ra_key",
            "contact_slot",
            "whatsapp_message",
        }
        missing = sorted(required.difference(df.columns))
        if missing:
            raise KeyError("Campanha sem colunas obrigatorias: " + ", ".join(missing))
        return df.copy()

    def _prepare_campaign_df(self, campaign_df: pd.DataFrame) -> pd.DataFrame:
        prepared = campaign_df.copy()
        for column in [
            "campaign_id",
            "class_name",
            "student_name",
            "parent_name",
            "ra_key",
            "contact_slot",
            "whatsapp_message",
            "status_envio",
            "status_resposta",
            "observacao",
        ]:
            if column in prepared.columns:
                prepared[column] = prepared[column].apply(self._safe_text)
        prepared["phone_sanitized"] = prepared["phone_sanitized"].apply(self._sanitize_phone)
        prepared["data_envio_dt"] = pd.to_datetime(prepared.get("data_envio"), errors="coerce")
        prepared["campaign_key"] = prepared.apply(self._build_campaign_key, axis=1)
        prepared["student_name_norm"] = prepared["student_name"].map(self._normalize_text)
        prepared["parent_name_norm"] = prepared["parent_name"].map(self._normalize_text)
        prepared["whatsapp_message_norm"] = prepared["whatsapp_message"].map(self._normalize_text)
        prepared["phone_group_size"] = prepared.groupby("phone_sanitized")["campaign_key"].transform("count")
        return prepared

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

    def _parse_export_file(self, file_path: Path, campaign_id: str) -> dict[str, object]:
        text = self._read_text_file(file_path)
        lines = text.splitlines()

        source_phone_guess = self._extract_phone_from_name(file_path.stem)
        source_contact_guess = self._normalize_name_guess(file_path.stem)
        conversation_id = self._normalize_conversation_id(file_path.stem)

        entries: list[dict[str, object]] = []
        current_entry: Optional[dict[str, object]] = None

        for raw_line in lines:
            parsed = self._parse_message_line(raw_line)
            if parsed is None:
                if current_entry is not None:
                    continuation = self._repair_text(raw_line.strip())
                    if continuation:
                        current_entry["message_text"] = (
                            f"{current_entry['message_text']}\n{continuation}".strip()
                        )
                        current_entry["message_text_norm"] = self._normalize_text(current_entry["message_text"])
                continue

            if current_entry is not None:
                entries.append(current_entry)
            current_entry = {
                "campaign_id": campaign_id,
                "conversation_id": conversation_id,
                "source_file": file_path.name,
                "source_file_path": str(file_path),
                "conversation_header": file_path.stem,
                "source_phone_guess": source_phone_guess,
                "source_contact_guess": source_contact_guess,
                "message_datetime": parsed["message_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
                "message_date": parsed["message_datetime"].strftime("%Y-%m-%d"),
                "message_time": parsed["message_datetime"].strftime("%H:%M:%S"),
                "author_label": parsed["author_label"],
                "author_label_norm": self._normalize_text(parsed["author_label"]),
                "author_is_school": self._is_school_author(parsed["author_label"]),
                "message_text": parsed["message_text"],
                "message_text_norm": self._normalize_text(parsed["message_text"]),
            }

        if current_entry is not None:
            entries.append(current_entry)

        messages_df = (
            pd.DataFrame(entries, columns=self._message_columns())
            if entries
            else pd.DataFrame(columns=self._message_columns())
        )
        file_record = {
            "campaign_id": campaign_id,
            "conversation_id": conversation_id,
            "source_file": file_path.name,
            "source_file_path": str(file_path),
            "conversation_header": file_path.stem,
            "source_phone_guess": source_phone_guess,
            "source_contact_guess": source_contact_guess,
            "message_count": len(messages_df),
            "school_message_count": int(messages_df["author_is_school"].fillna(False).sum()) if not messages_df.empty else 0,
        }
        return {
            "file_path": file_path,
            "conversation_id": conversation_id,
            "messages_df": messages_df,
            "file_record": file_record,
        }

    def _build_matches(
        self,
        campaign_df: pd.DataFrame,
        conversations: list[dict[str, object]],
    ) -> pd.DataFrame:
        match_rows: list[dict[str, object]] = []

        for conversation in conversations:
            messages_df = conversation["messages_df"]
            file_record = conversation["file_record"]
            source_phone = self._safe_text(file_record["source_phone_guess"])
            source_contact_guess = self._safe_text(file_record["source_contact_guess"])

            conversation_matches: list[dict[str, object]] = []
            school_messages_df = messages_df.loc[messages_df["author_is_school"].fillna(False)].copy()

            phone_candidates = pd.DataFrame(columns=campaign_df.columns)
            if source_phone:
                phone_candidates = campaign_df.loc[campaign_df["phone_sanitized"].eq(source_phone)].copy()

            for _, campaign_row in campaign_df.iterrows():
                row_match = self._evaluate_contact_match(
                    campaign_row=campaign_row,
                    phone_candidates=phone_candidates,
                    source_phone=source_phone,
                    source_contact_guess=source_contact_guess,
                    school_messages_df=school_messages_df,
                    file_record=file_record,
                )
                if row_match is not None:
                    conversation_matches.append(row_match)

            conversation_matches = self._assign_reply_windows(conversation_matches, messages_df)
            match_rows.extend(conversation_matches)

        matches_df = (
            pd.DataFrame(match_rows, columns=self._match_columns())
            if match_rows
            else pd.DataFrame(columns=self._match_columns())
        )
        if not matches_df.empty:
            matches_df = matches_df.sort_values(
                ["source_file", "campaign_prompt_datetime", "matched_student_name", "matched_phone"],
                na_position="last",
            ).reset_index(drop=True)
        return matches_df

    def _evaluate_contact_match(
        self,
        campaign_row: pd.Series,
        phone_candidates: pd.DataFrame,
        source_phone: str,
        source_contact_guess: str,
        school_messages_df: pd.DataFrame,
        file_record: dict[str, object],
    ) -> Optional[dict[str, object]]:
        student_name = self._safe_text(campaign_row.get("student_name"))
        student_name_norm = self._safe_text(campaign_row.get("student_name_norm"))
        parent_name_norm = self._safe_text(campaign_row.get("parent_name_norm"))
        whatsapp_message_norm = self._safe_text(campaign_row.get("whatsapp_message_norm"))
        campaign_key = self._safe_text(campaign_row.get("campaign_key"))
        phone_group_size = int(campaign_row.get("phone_group_size") or 0)

        prompt_candidate = self._find_best_prompt(
            school_messages_df=school_messages_df,
            student_name_norm=student_name_norm,
            whatsapp_message_norm=whatsapp_message_norm,
        )

        match_method = ""
        needs_review_reasons: list[str] = []
        matched_score = 0.0

        phone_unique = bool(source_phone) and phone_group_size == 1 and self._safe_text(campaign_row.get("phone_sanitized")) == source_phone
        phone_shared = bool(source_phone) and phone_group_size > 1 and self._safe_text(campaign_row.get("phone_sanitized")) == source_phone
        parent_match = (
            self._is_specific_contact_name(source_contact_guess)
            and self._is_specific_contact_name(parent_name_norm)
            and (
            source_contact_guess in parent_name_norm or parent_name_norm in source_contact_guess
            )
        )

        if prompt_candidate is not None:
            match_method = "student_name_from_school_message"
            matched_score = float(prompt_candidate["match_score"])
            if phone_shared:
                needs_review_reasons.append("telefone_compartilhado")
            if source_phone and self._safe_text(campaign_row.get("phone_sanitized")) != source_phone:
                needs_review_reasons.append("prompt_em_conversa_com_outro_numero")
        elif phone_unique:
            match_method = "phone"
            matched_score = 0.7
        elif parent_match and not phone_shared:
            match_method = "parent_name_header"
            matched_score = 0.55
        elif phone_shared:
            return None
        else:
            return None

        has_campaign_prompt = prompt_candidate is not None
        if not has_campaign_prompt:
            needs_review_reasons.append("sem_prompt_identificado")
        if match_method == "parent_name_header":
            needs_review_reasons.append("match_por_nome_do_cabecalho")

        data_envio_dt = pd.to_datetime(campaign_row.get("data_envio_dt"), errors="coerce")
        prompt_dt = pd.to_datetime(prompt_candidate["message_datetime"], errors="coerce") if prompt_candidate else pd.NaT

        return {
            "campaign_id": self._safe_text(campaign_row.get("campaign_id")),
            "campaign_key": campaign_key,
            "source_file": self._safe_text(file_record["source_file"]),
            "source_file_path": self._safe_text(file_record["source_file_path"]),
            "conversation_id": self._safe_text(file_record["conversation_id"]),
            "conversation_header": self._safe_text(file_record["conversation_header"]),
            "match_method": match_method,
            "match_score": matched_score,
            "matched_student_name": student_name,
            "matched_parent_name": self._safe_text(campaign_row.get("parent_name")),
            "matched_phone": self._safe_text(campaign_row.get("phone_sanitized")),
            "matched_contact_slot": self._safe_text(campaign_row.get("contact_slot")),
            "matched_ra_key": self._safe_text(campaign_row.get("ra_key")),
            "data_envio": data_envio_dt.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(data_envio_dt) else "",
            "has_campaign_prompt": has_campaign_prompt,
            "campaign_prompt_datetime": prompt_dt.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(prompt_dt) else "",
            "campaign_prompt_text": self._safe_text(prompt_candidate["message_text"]) if prompt_candidate else "",
            "campaign_prompt_author": self._safe_text(prompt_candidate["author_label"]) if prompt_candidate else "",
            "first_reply_after_send_datetime": "",
            "first_reply_author": "",
            "first_reply_text": "",
            "reply_count_after_send": 0,
            "reason_category_suggested": "",
            "reason_text_excerpt": "",
            "needs_review": bool(needs_review_reasons),
            "review_reason": " | ".join(dict.fromkeys(needs_review_reasons)),
        }

    def _assign_reply_windows(
        self,
        conversation_matches: list[dict[str, object]],
        messages_df: pd.DataFrame,
    ) -> list[dict[str, object]]:
        if not conversation_matches:
            return conversation_matches

        messages = messages_df.copy()
        messages["message_datetime_dt"] = pd.to_datetime(messages["message_datetime"], errors="coerce")
        incoming_messages = messages.loc[
            messages["message_datetime_dt"].notna()
            & ~messages["author_is_school"].fillna(False)
            & messages["author_label_norm"].ne("system")
        ].copy()
        incoming_messages = incoming_messages.sort_values("message_datetime_dt")

        ordered_matches = sorted(
            conversation_matches,
            key=lambda row: (
                self._sort_datetime_value(row.get("campaign_prompt_datetime")),
                self._sort_datetime_value(row.get("data_envio")),
                row.get("matched_student_name", ""),
            ),
        )

        for index, match_row in enumerate(ordered_matches):
            start_dt = self._coalesce_datetime(
                match_row.get("campaign_prompt_datetime"),
                match_row.get("data_envio"),
            )
            next_prompt_dt = None
            for later_match in ordered_matches[index + 1 :]:
                next_prompt_dt = self._parse_optional_datetime(later_match.get("campaign_prompt_datetime"))
                if next_prompt_dt is not None:
                    break

            candidate_replies = incoming_messages.copy()
            if start_dt is not None:
                candidate_replies = candidate_replies.loc[candidate_replies["message_datetime_dt"] >= start_dt]
            if next_prompt_dt is not None:
                candidate_replies = candidate_replies.loc[candidate_replies["message_datetime_dt"] < next_prompt_dt]

            if candidate_replies.empty and not match_row["has_campaign_prompt"]:
                data_envio_dt = self._parse_optional_datetime(match_row.get("data_envio"))
                candidate_replies = incoming_messages.copy()
                if data_envio_dt is not None:
                    candidate_replies = candidate_replies.loc[candidate_replies["message_datetime_dt"] >= data_envio_dt]

            if len(ordered_matches) > 1 and not match_row["has_campaign_prompt"]:
                match_row["needs_review"] = True
                match_row["review_reason"] = self._append_reason(
                    match_row.get("review_reason", ""),
                    "telefone_compartilhado_sem_segmentacao",
                )

            if not candidate_replies.empty:
                first_reply = candidate_replies.iloc[0]
                match_row["first_reply_after_send_datetime"] = self._safe_text(first_reply["message_datetime"])
                match_row["first_reply_author"] = self._safe_text(first_reply["author_label"])
                match_row["first_reply_text"] = self._safe_text(first_reply["message_text"])
                match_row["reply_count_after_send"] = int(len(candidate_replies))
                category, excerpt = self._classify_reason(candidate_replies["message_text"].tolist())
                match_row["reason_category_suggested"] = category
                match_row["reason_text_excerpt"] = excerpt
                if category in {"outros", "sem_justificativa"}:
                    match_row["needs_review"] = True
                    match_row["review_reason"] = self._append_reason(
                        match_row.get("review_reason", ""),
                        "justificativa_precisa_revisao",
                    )

        return ordered_matches

    def _find_best_prompt(
        self,
        school_messages_df: pd.DataFrame,
        student_name_norm: str,
        whatsapp_message_norm: str,
    ) -> Optional[dict[str, object]]:
        if school_messages_df.empty:
            return None

        best_row = None
        best_score = 0.0
        student_tokens = [token for token in student_name_norm.split() if len(token) >= 3]

        for _, row in school_messages_df.iterrows():
            text_norm = self._safe_text(row.get("message_text_norm"))
            if not text_norm:
                continue

            score = 0.0
            if student_name_norm and student_name_norm in text_norm:
                score += 1.0
            elif student_tokens:
                token_hits = sum(1 for token in student_tokens if token in text_norm)
                score += min(token_hits / max(len(student_tokens), 1), 1.0) * 0.8

            if whatsapp_message_norm:
                overlap = self._token_overlap_ratio(whatsapp_message_norm, text_norm)
                score += overlap * 0.6

            if score > best_score:
                best_score = score
                best_row = row

        if best_row is None or best_score < 0.75:
            return None

        result = best_row.to_dict()
        result["match_score"] = round(best_score, 3)
        return result

    def _build_files_df(self, file_records: list[dict[str, object]], matches_df: pd.DataFrame) -> pd.DataFrame:
        files_df = pd.DataFrame(file_records, columns=self._file_columns())
        if files_df.empty:
            return files_df

        if matches_df.empty:
            files_df["matched_contact_count"] = 0
            files_df["has_campaign_prompt"] = False
            files_df["needs_review"] = False
            files_df["review_reason"] = ""
            return files_df

        summary_df = (
            matches_df.groupby("source_file", dropna=False)
            .agg(
                matched_contact_count=("campaign_key", "count"),
                has_campaign_prompt=("has_campaign_prompt", "any"),
                needs_review=("needs_review", "any"),
                review_reason=("review_reason", lambda values: " | ".join(sorted({self._safe_text(v) for v in values if self._safe_text(v)}))),
            )
            .reset_index()
        )
        return files_df.merge(summary_df, on="source_file", how="left").fillna(
            {
                "matched_contact_count": 0,
                "has_campaign_prompt": False,
                "needs_review": False,
                "review_reason": "",
            }
        )

    def _write_output(
        self,
        messages_df: pd.DataFrame,
        files_df: pd.DataFrame,
        matches_df: pd.DataFrame,
        output_path: Path,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            messages_df.to_excel(writer, sheet_name="Messages", index=False)
            files_df.to_excel(writer, sheet_name="Files", index=False)
            matches_df.to_excel(writer, sheet_name="Matches", index=False)

    def _load_reason_taxonomy(self) -> dict[str, list[str]]:
        taxonomy_path = Path(__file__).with_name("reason_taxonomy.json")
        if not taxonomy_path.exists():
            return {}
        with taxonomy_path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        return {
            self._normalize_text(category): [self._normalize_text(keyword) for keyword in keywords]
            for category, keywords in raw.items()
        }

    def _classify_reason(self, messages: list[str]) -> tuple[str, str]:
        joined = " \n ".join(self._safe_text(message) for message in messages if self._safe_text(message))
        joined_norm = self._normalize_text(joined)
        if not joined_norm:
            return "sem_justificativa", ""

        for category, keywords in self.taxonomy.items():
            for keyword in keywords:
                if keyword and keyword in joined_norm:
                    return category, self._extract_excerpt(joined, keyword)
        return "outros", joined[:200].strip()

    def _extract_excerpt(self, original_text: str, keyword_norm: str) -> str:
        normalized = self._normalize_text(original_text)
        if not keyword_norm or keyword_norm not in normalized:
            return original_text[:200].strip()
        return original_text[:200].strip()

    def _parse_message_line(self, line: str) -> Optional[dict[str, object]]:
        repaired_line = self._repair_text(line.strip())
        for pattern in TIMESTAMP_PATTERNS:
            match = pattern.match(repaired_line)
            if not match:
                continue

            date_text = match.group("date").strip()
            time_text = match.group("time").strip()
            body = self._repair_text(match.group("body").strip())
            message_datetime = self._parse_datetime(date_text, time_text)

            author_label = ""
            message_text = body
            if ": " in body:
                author_label, message_text = body.split(": ", 1)
            if not author_label and any(marker in self._normalize_text(message_text) for marker in SYSTEM_MESSAGE_MARKERS):
                author_label = "system"

            return {
                "message_datetime": message_datetime,
                "author_label": self._repair_text(author_label.strip()),
                "message_text": self._repair_text(message_text.strip()),
            }
        return None

    def _parse_datetime(self, date_text: str, time_text: str) -> datetime:
        combined = f"{date_text} {time_text}"
        for fmt in DATETIME_FORMATS:
            try:
                return datetime.strptime(combined, fmt)
            except ValueError:
                continue
        raise ValueError(f"Data/hora invalida na exportacao: {combined}")

    @staticmethod
    def _read_text_file(path: Path) -> str:
        for encoding in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                return path.read_text(encoding=encoding)
            except UnicodeDecodeError:
                continue
        return path.read_text(encoding="utf-8", errors="replace")

    @staticmethod
    def _repair_text(value: object) -> str:
        text = "" if value is None else str(value)
        if not text:
            return ""
        if any(marker in text for marker in ("Ã", "Â", "â")):
            try:
                repaired = text.encode("latin-1").decode("utf-8")
                if repaired:
                    return repaired
            except (UnicodeEncodeError, UnicodeDecodeError):
                return text
        return text

    @staticmethod
    def _normalize_text(value: object) -> str:
        text = WhatsAppExportParser._repair_text(value).strip().lower()
        if not text:
            return ""
        text = unicodedata.normalize("NFKD", text)
        text = "".join(char for char in text if not unicodedata.combining(char))
        text = re.sub(r"[^\w\s]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _normalize_name_guess(raw_name: str) -> str:
        cleaned = re.sub(r"^conversa do whatsapp com", "", raw_name, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"\+?\d[\d\s\-\(\)]{7,}", "", cleaned).strip(" -_")
        return WhatsAppExportParser._normalize_text(cleaned)

    @staticmethod
    def _is_specific_contact_name(value: str) -> bool:
        normalized = WhatsAppExportParser._normalize_text(value)
        if not normalized:
            return False
        tokens = [token for token in normalized.split() if token]
        if not tokens:
            return False
        if len(tokens) == 1 and tokens[0] in GENERIC_PARENT_LABELS:
            return False
        return len("".join(tokens)) >= 6

    @staticmethod
    def _normalize_conversation_id(raw_name: str) -> str:
        normalized = WhatsAppExportParser._normalize_text(raw_name)
        return normalized or raw_name.strip()

    @staticmethod
    def _sanitize_phone(value: object) -> str:
        digits = re.sub(r"\D", "", WhatsAppExportParser._safe_text(value))
        return digits

    @staticmethod
    def _extract_phone_from_name(raw_name: str) -> str:
        digits = re.sub(r"\D", "", raw_name)
        return digits if len(digits) >= 10 else ""

    @staticmethod
    def _safe_text(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        return WhatsAppExportParser._repair_text(str(value)).strip()

    @staticmethod
    def _build_campaign_key(row: pd.Series) -> str:
        return WhatsAppExportParser._join_key(
            row.get("ra_key"),
            row.get("phone_sanitized"),
            row.get("contact_slot"),
        )

    @staticmethod
    def _join_key(*parts: object) -> str:
        return "|".join(WhatsAppExportParser._safe_text(part) for part in parts)

    @staticmethod
    def _token_overlap_ratio(left_text: str, right_text: str) -> float:
        left_tokens = {token for token in left_text.split() if len(token) >= 3}
        right_tokens = {token for token in right_text.split() if len(token) >= 3}
        if not left_tokens or not right_tokens:
            return 0.0
        return len(left_tokens & right_tokens) / len(left_tokens)

    @staticmethod
    def _is_school_author(author_label: str) -> bool:
        norm = WhatsAppExportParser._normalize_text(author_label)
        if not norm:
            return False
        if norm == "system":
            return False
        school_markers = ("escola", "decia", "direcao", "secretaria")
        return any(marker in norm for marker in school_markers) and not norm.startswith("55 ")

    @staticmethod
    def _parse_optional_datetime(value: object) -> Optional[datetime]:
        text = WhatsAppExportParser._safe_text(value)
        if not text:
            return None
        dt = pd.to_datetime(text, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()

    @staticmethod
    def _coalesce_datetime(*values: object) -> Optional[datetime]:
        for value in values:
            dt = WhatsAppExportParser._parse_optional_datetime(value)
            if dt is not None:
                return dt
        return None

    @staticmethod
    def _sort_datetime_value(value: object) -> tuple[int, str]:
        dt = WhatsAppExportParser._parse_optional_datetime(value)
        if dt is None:
            return (1, "")
        return (0, dt.isoformat())

    @staticmethod
    def _append_reason(current: str, new_reason: str) -> str:
        pieces = [piece.strip() for piece in current.split("|") if piece.strip()]
        if new_reason and new_reason not in pieces:
            pieces.append(new_reason)
        return " | ".join(pieces)

    @staticmethod
    def _message_columns() -> list[str]:
        return [
            "campaign_id",
            "conversation_id",
            "source_file",
            "source_file_path",
            "conversation_header",
            "source_phone_guess",
            "source_contact_guess",
            "message_datetime",
            "message_date",
            "message_time",
            "author_label",
            "author_label_norm",
            "author_is_school",
            "message_text",
            "message_text_norm",
        ]

    @staticmethod
    def _file_columns() -> list[str]:
        return [
            "campaign_id",
            "conversation_id",
            "source_file",
            "source_file_path",
            "conversation_header",
            "source_phone_guess",
            "source_contact_guess",
            "message_count",
            "school_message_count",
        ]

    @staticmethod
    def _match_columns() -> list[str]:
        return [
            "campaign_id",
            "campaign_key",
            "source_file",
            "source_file_path",
            "conversation_id",
            "conversation_header",
            "match_method",
            "match_score",
            "matched_student_name",
            "matched_parent_name",
            "matched_phone",
            "matched_contact_slot",
            "matched_ra_key",
            "data_envio",
            "has_campaign_prompt",
            "campaign_prompt_datetime",
            "campaign_prompt_text",
            "campaign_prompt_author",
            "first_reply_after_send_datetime",
            "first_reply_author",
            "first_reply_text",
            "reply_count_after_send",
            "reason_category_suggested",
            "reason_text_excerpt",
            "needs_review",
            "review_reason",
        ]


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parseia exportacoes .txt do WhatsApp e cruza com uma campanha.",
    )
    parser.add_argument(
        "--campaign",
        required=True,
        help="Caminho do arquivo Excel da campanha.",
    )
    parser.add_argument(
        "--exports-dir",
        required=True,
        help="Pasta raiz das exportacoes do WhatsApp.",
    )
    parser.add_argument(
        "--output",
        help="Caminho opcional do arquivo Excel de saida.",
    )
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    parser = WhatsAppExportParser(
        campaign_path=Path(args.campaign),
        exports_dir=Path(args.exports_dir),
    )
    result = parser.run(output_path=Path(args.output) if args.output else None)
    logger.info(
        "Parser finalizado | campaign_id=%s | mensagens=%s | arquivos_casados=%s | arquivos_sem_casamento=%s",
        result.campaign_id,
        result.message_count,
        result.matched_file_count,
        result.unmatched_file_count,
    )


if __name__ == "__main__":
    main()
