from urllib.parse import quote


class WhatsAppLinkBuilder:
    def __init__(self, message_template: str) -> None:
        self.message_template = message_template

    def build_message(self, parent_name: str, student_name: str, absence_days: str = "") -> str:
        return self.message_template.format(
            parent_name=(parent_name or "responsável").strip(),
            student_name=(student_name or "aluno(a)").strip(),
            absence_days=(absence_days or "dias não informados").strip(),
        )

    def build_link(self, phone_number: str, message: str) -> str:
        clean_phone = "".join(filter(str.isdigit, str(phone_number or "")))
        encoded_message = quote(message)
        return f"https://wa.me/{clean_phone}?text={encoded_message}"
