import hashlib
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MessageTemplate:
    template_id: str
    text: str


MESSAGE_TEMPLATES = [
    MessageTemplate(
        template_id="msg_01",
        text="Ola {parent_name}, aqui e da {school_name}. O(a) aluno(a) {student_name}, da turma {class_name}, esteve ausente nos dias {absence_days}. Poderia nos informar o motivo ou entrar em contato com a escola?",
    ),
    MessageTemplate(
        template_id="msg_02",
        text="Bom dia/boa tarde, {parent_name}. A {school_name} informa que {student_name}, da turma {class_name}, faltou as aulas nos dias {absence_days}. Pedimos a gentileza de justificar ou procurar a secretaria.",
    ),
    MessageTemplate(
        template_id="msg_03",
        text="Ola, {parent_name}. A {school_name} informa que {student_name}, da turma {class_name}, registrou faltas nos dias {absence_days}. Favor nos comunicar o motivo das ausencias.",
    ),
    MessageTemplate(
        template_id="msg_04",
        text="Prezado(a) {parent_name}, a {school_name} identificou que o(a) estudante {student_name}, da turma {class_name}, nao compareceu as aulas nos dias {absence_days}. Solicitamos contato com a escola para esclarecimentos.",
    ),
    MessageTemplate(
        template_id="msg_05",
        text="Ola {parent_name}, tudo bem? A {school_name} notou que {student_name}, da turma {class_name}, faltou as aulas nos dias {absence_days}. Poderia nos informar se esta tudo certo?",
    ),
    MessageTemplate(
        template_id="msg_06",
        text="Oi {parent_name}, aqui e da {school_name}. Observamos a ausencia de {student_name}, da turma {class_name}, nos dias {absence_days}. Caso possa, pedimos que nos informe o motivo.",
    ),
    MessageTemplate(
        template_id="msg_07",
        text="Ola {parent_name}, esperamos que esteja bem. A {school_name} identificou faltas de {student_name}, da turma {class_name}, nos dias {absence_days}. Estamos a disposicao para qualquer esclarecimento.",
    ),
    MessageTemplate(
        template_id="msg_08",
        text="Ola {parent_name}, a {school_name} entrou em contato porque {student_name}, da turma {class_name}, esteve ausente nos dias {absence_days}. Caso precise de apoio, conte conosco.",
    ),
    MessageTemplate(
        template_id="msg_09",
        text="Bom dia/boa tarde, {parent_name}. A {school_name} notou as ausencias de {student_name}, da turma {class_name}, nos dias {absence_days}. Por favor, nos informe o motivo ou procure a escola.",
    ),
    MessageTemplate(
        template_id="msg_10",
        text="Ola {parent_name}, a {school_name} verificou que {student_name}, da turma {class_name}, nao compareceu as aulas nos dias {absence_days}. Pedimos retorno para atualizacao da situacao.",
    ),
    MessageTemplate(
        template_id="msg_11",
        text="{parent_name}, a {school_name} informa que o(a) aluno(a) {student_name}, da turma {class_name}, apresentou faltas nos dias {absence_days}. Favor justificar junto a escola.",
    ),
    MessageTemplate(
        template_id="msg_12",
        text="Prezados responsaveis, a {school_name} registrou ausencia de {student_name}, da turma {class_name}, nas datas {absence_days}. Solicitamos contato com a escola.",
    ),
    MessageTemplate(
        template_id="msg_13",
        text="A {school_name} comunica que {student_name}, da turma {class_name}, esteve ausente nos dias {absence_days}. Aguardamos justificativa ou contato da familia.",
    ),
    MessageTemplate(
        template_id="msg_14",
        text="Ola {parent_name}, a {school_name} realiza acompanhamento de frequencia e identificou faltas de {student_name}, da turma {class_name}, nos dias {absence_days}. Por favor, informe o motivo.",
    ),
    MessageTemplate(
        template_id="msg_15",
        text="A {school_name} esta entrando em contato para acompanhar a frequencia de {student_name}, da turma {class_name}, ausente nos dias {absence_days}. Caso necessario, a escola esta a disposicao.",
    ),
    MessageTemplate(
        template_id="msg_16",
        text="Ola {parent_name}, a {school_name} verificou ausencias de {student_name}, da turma {class_name}, nos dias {absence_days}. Este contato e para acompanhamento e apoio escolar.",
    ),
    MessageTemplate(
        template_id="msg_17",
        text="Oi {parent_name}, tudo bem? Aqui e da {school_name}. O(a) {student_name}, da turma {class_name}, faltou nos dias {absence_days}. Pode nos dizer se esta tudo certo?",
    ),
    MessageTemplate(
        template_id="msg_18",
        text="Ola {parent_name}! A {school_name} sentiu falta de {student_name}, da turma {class_name}, nos dias {absence_days}. Por favor, nos informe o motivo das ausencias.",
    ),
    MessageTemplate(
        template_id="msg_19",
        text="Bom dia/boa tarde, {parent_name}. A {school_name} notou que {student_name}, da turma {class_name}, nao esteve presente nos dias {absence_days}. Aguardamos seu retorno.",
    ),
    MessageTemplate(
        template_id="msg_20",
        text="Ola {parent_name}, a {school_name} esta verificando a frequencia dos alunos e viu que {student_name}, da turma {class_name}, faltou nos dias {absence_days}. Poderia nos dar um retorno?",
    ),
]


class MessageCatalog:
    def __init__(self, school_name: str = "Escola Decia") -> None:
        self.templates = MESSAGE_TEMPLATES
        self.school_name = (school_name or "Escola Decia").strip() or "Escola Decia"

    def build_message(
        self,
        parent_name: str,
        student_name: str,
        class_name: str,
        absence_days: str,
        campaign_id: str,
        unique_key: str,
    ) -> tuple[str, str]:
        template = self._choose_template(campaign_id=campaign_id, unique_key=unique_key)
        message = template.text.format(
            parent_name=(parent_name or "Responsavel").strip(),
            student_name=(student_name or "Aluno(a)").strip(),
            class_name=self._normalize_class_name_short(class_name),
            absence_days=(absence_days or "dias nao informados").strip(),
            school_name=self.school_name,
        )
        return template.template_id, message

    def _choose_template(self, campaign_id: str, unique_key: str) -> MessageTemplate:
        seed = f"{campaign_id}|{unique_key}".encode("utf-8")
        digest = hashlib.sha256(seed).hexdigest()
        index = int(digest[:8], 16) % len(self.templates)
        return self.templates[index]

    @staticmethod
    def _normalize_class_name_short(value: str) -> str:
        text = str(value or "").strip().upper()
        if not text:
            return "nao informada"
        text = re.sub(r"^\s*TURMA\s+", "", text)
        match = re.search(r"\b([6-9])\s*ANO\b.*?\b(?:[6-9]\s*)?([A-Z])\b", text)
        if match:
            return f"{match.group(1)} ANO {match.group(2)}"
        match_short = re.search(r"\b([6-9])\s*ANO\s+([A-Z])\b", text)
        if match_short:
            return f"{match_short.group(1)} ANO {match_short.group(2)}"
        return text or "nao informada"
