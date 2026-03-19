import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class MessageTemplate:
    template_id: str
    text: str


MESSAGE_TEMPLATES = [
    MessageTemplate(
        template_id="msg_01",
        text="Ola {parent_name}, aqui e da Escola Decia. O(a) aluno(a) {student_name} esteve ausente nos dias {absence_days}. Poderia nos informar o motivo ou entrar em contato com a escola?",
    ),
    MessageTemplate(
        template_id="msg_02",
        text="Bom dia/boa tarde, {parent_name}. Informamos que {student_name} faltou as aulas nos dias {absence_days}. Pedimos a gentileza de justificar ou procurar a secretaria.",
    ),
    MessageTemplate(
        template_id="msg_03",
        text="Ola, {parent_name}. A escola Decia informa que {student_name} registrou faltas nos dias {absence_days}. Favor nos comunicar o motivo das ausencias.",
    ),
    MessageTemplate(
        template_id="msg_04",
        text="Prezado(a) {parent_name}, o(a) estudante {student_name} nao compareceu as aulas nos dias {absence_days}. Solicitamos contato com a escola para esclarecimentos.",
    ),
    MessageTemplate(
        template_id="msg_05",
        text="Ola {parent_name}, tudo bem? Notamos que {student_name} faltou as aulas nos dias {absence_days}. Poderia nos informar se esta tudo certo?",
    ),
    MessageTemplate(
        template_id="msg_06",
        text="Oi {parent_name}, aqui e da Escola Decia. Observamos a ausencia de {student_name} nos dias {absence_days}. Caso possa, pedimos que nos informe o motivo.",
    ),
    MessageTemplate(
        template_id="msg_07",
        text="Ola {parent_name}, esperamos que esteja bem. Identificamos faltas de {student_name} nos dias {absence_days}. Estamos a disposicao para qualquer esclarecimento.",
    ),
    MessageTemplate(
        template_id="msg_08",
        text="Ola {parent_name}, a escola Decia entrou em contato pois {student_name} esteve ausente nos dias {absence_days}. Caso precise de apoio, conte conosco.",
    ),
    MessageTemplate(
        template_id="msg_09",
        text="Bom dia/boa tarde, {parent_name}. Notamos as ausencias de {student_name} nos dias {absence_days}. Por favor, nos informe o motivo ou procure a escola.",
    ),
    MessageTemplate(
        template_id="msg_10",
        text="Ola {parent_name}, verificamos que {student_name} nao compareceu as aulas nos dias {absence_days}. Pedimos retorno para atualizacao da situacao.",
    ),
    MessageTemplate(
        template_id="msg_11",
        text="{parent_name}, informamos que o(a) aluno(a) {student_name} apresentou faltas nos dias {absence_days}. Favor justificar junto a Escola Decia.",
    ),
    MessageTemplate(
        template_id="msg_12",
        text="Prezados responsaveis, o(a) estudante {student_name} registrou ausencia nas datas {absence_days}. Solicitamos contato com a unidade escolar.",
    ),
    MessageTemplate(
        template_id="msg_13",
        text="Comunicamos que {student_name} esteve ausente nos dias {absence_days}. Aguardamos justificativa ou contato da familia.",
    ),
    MessageTemplate(
        template_id="msg_14",
        text="Ola {parent_name}, a escola Decia realiza acompanhamento de frequencia e identificou faltas de {student_name} nos dias {absence_days}. Por favor, informe o motivo.",
    ),
    MessageTemplate(
        template_id="msg_15",
        text="Estamos entrando em contato para acompanhar a frequencia de {student_name}, ausente nos dias {absence_days}. Caso necessario, a escola esta a disposicao.",
    ),
    MessageTemplate(
        template_id="msg_16",
        text="Ola {parent_name}, verificamos ausencias de {student_name} nos dias {absence_days}. O contato e para acompanhamento e apoio escolar.",
    ),
    MessageTemplate(
        template_id="msg_17",
        text="Oi {parent_name}, tudo bem? Aqui e da escola Decia. O(a) {student_name} faltou nos dias {absence_days}. Pode nos dizer se esta tudo certo?",
    ),
    MessageTemplate(
        template_id="msg_18",
        text="Ola {parent_name}! Sentimos falta de {student_name} na escola nos dias {absence_days}. Por favor, nos informe o motivo das ausencias.",
    ),
    MessageTemplate(
        template_id="msg_19",
        text="Bom dia/boa tarde, {parent_name}. Notamos que {student_name} nao esteve presente nos dias {absence_days}. Aguardo seu retorno.",
    ),
    MessageTemplate(
        template_id="msg_20",
        text="Ola {parent_name}, estamos verificando a frequencia dos alunos e vimos que {student_name} faltou nos dias {absence_days}. Poderia nos dar um retorno?",
    ),
]


class MessageCatalog:
    def __init__(self) -> None:
        self.templates = MESSAGE_TEMPLATES

    def build_message(
        self,
        parent_name: str,
        student_name: str,
        absence_days: str,
        campaign_id: str,
        unique_key: str,
    ) -> tuple[str, str]:
        template = self._choose_template(campaign_id=campaign_id, unique_key=unique_key)
        message = template.text.format(
            parent_name=(parent_name or "Responsavel").strip(),
            student_name=(student_name or "Aluno(a)").strip(),
            absence_days=(absence_days or "dias nao informados").strip(),
        )
        return template.template_id, message

    def _choose_template(self, campaign_id: str, unique_key: str) -> MessageTemplate:
        seed = f"{campaign_id}|{unique_key}".encode("utf-8")
        digest = hashlib.sha256(seed).hexdigest()
        index = int(digest[:8], 16) % len(self.templates)
        return self.templates[index]
