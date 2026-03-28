import hashlib
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class InstitutionalMessageTemplate:
    template_id: str
    text: str


INSTITUTIONAL_MESSAGE_TEMPLATES = [
    InstitutionalMessageTemplate(
        template_id="inst_01",
        text="A Direcao da {school_name} comunica aos responsaveis de {student_name}, da turma {class_name_short}, que iniciamos o periodo de avaliacoes do bimestre. Havera provas diariamente ate 17 de abril. Pedimos que evitem faltas para nao prejudicar o rendimento dos alunos.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_02",
        text="Informamos aos responsaveis de {student_name}, da turma {class_name_short}, que comecaram as avaliacoes bimestrais na {school_name}. As provas ocorrerao todos os dias ate 17/04. E muito importante a presenca dos estudantes nesse periodo.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_03",
        text="Atencao, responsaveis de {student_name}, da turma {class_name_short}: a {school_name} esta em periodo de provas para fechamento das medias. As avaliacoes serao diarias ate 17 de abril. Contamos com a presenca dos alunos.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_04",
        text="A {school_name} entrou na fase de avaliacoes bimestrais para {student_name}, da turma {class_name_short}, com provas programadas diariamente ate 17/04. Evitem ausencias para nao comprometer o boletim.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_05",
        text="Comunicamos aos responsaveis de {student_name}, da turma {class_name_short}, que ate o dia 17 de abril teremos avaliacoes todos os dias na {school_name}. A participacao dos alunos e essencial para o fechamento das notas.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_06",
        text="A Direcao da {school_name} solicita atencao ao periodo de provas de {student_name}, da turma {class_name_short}, que vai ate 17/04. Faltas podem prejudicar as medias finais dos estudantes.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_07",
        text="Informamos que as avaliacoes do bimestre de {student_name}, da turma {class_name_short}, ja comecaram na {school_name} e acontecerao diariamente ate 17 de abril. Pedimos que os alunos nao faltem.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_08",
        text="Prezados responsaveis de {student_name}, da turma {class_name_short}, a {school_name} esta em semana de provas continuas ate 17/04. A presenca e fundamental para o bom desempenho escolar.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_09",
        text="Comunicamos que ocorre neste momento o periodo avaliativo do bimestre para {student_name}, da turma {class_name_short}, na {school_name}. Havera provas todos os dias ate 17 de abril.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_10",
        text="Atencao: ate 17/04, {student_name}, da turma {class_name_short}, realizara avaliacoes diariamente na {school_name}. Pedimos apoio das familias para garantir a frequencia.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_11",
        text="A {school_name} informa que as provas bimestrais de {student_name}, da turma {class_name_short}, estao em andamento ate o dia 17 de abril. Evitem faltas nesse periodo.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_12",
        text="Estamos no periodo de fechamento das medias escolares de {student_name}, da turma {class_name_short}, na {school_name}. As avaliacoes acontecerao todos os dias ate 17/04.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_13",
        text="A {school_name} comunica as familias de {student_name}, da turma {class_name_short}, que havera provas diarias ate 17 de abril. A ausencia pode impactar diretamente o boletim.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_14",
        text="Informamos que o calendario de avaliacoes de {student_name}, da turma {class_name_short}, na {school_name}, segue ate 17/04, com atividades avaliativas todos os dias.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_15",
        text="A Direcao da {school_name} reforca a importancia da presenca de {student_name}, da turma {class_name_short}, nas provas que ocorrerao diariamente ate 17 de abril.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_16",
        text="Iniciamos o periodo avaliativo do bimestre para {student_name}, da turma {class_name_short}, na {school_name}. As avaliacoes serao continuas ate 17/04. Contamos com a colaboracao das familias.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_17",
        text="Prezados responsaveis, as provas para composicao das medias de {student_name}, da turma {class_name_short}, ocorrerao todos os dias ate 17 de abril na {school_name}.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_18",
        text="A {school_name} esta realizando avaliacoes diarias para {student_name}, da turma {class_name_short}, ate 17/04. E essencial que os alunos comparecam.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_19",
        text="Comunicamos que a {school_name} esta na fase final de avaliacao do bimestre de {student_name}, da turma {class_name_short}, com provas todos os dias ate 17/04.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_20",
        text="Informamos que as avaliacoes para fechamento das notas de {student_name}, da turma {class_name_short}, na {school_name}, estao acontecendo diariamente ate 17 de abril.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_21",
        text="A Direcao da {school_name} solicita atencao especial a frequencia de {student_name}, da turma {class_name_short}, durante o periodo de provas, que vai ate 17/04.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_22",
        text="As avaliacoes bimestrais de {student_name}, da turma {class_name_short}, na {school_name}, seguem ate 17 de abril, ocorrendo diariamente. Pedimos que evitem faltas.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_23",
        text="A {school_name} esta em periodo decisivo de avaliacoes escolares para {student_name}, da turma {class_name_short}, com provas todos os dias ate 17/04.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_24",
        text="Comunicamos que a ausencia de {student_name}, da turma {class_name_short}, nas provas ate 17 de abril na {school_name} podera comprometer o desempenho no boletim.",
    ),
    InstitutionalMessageTemplate(
        template_id="inst_25",
        text="A {school_name} informa que as avaliacoes do bimestre de {student_name}, da turma {class_name_short}, ocorrem diariamente ate 17/04 e sao fundamentais para a composicao das medias.",
    ),
]


class InstitutionalMessageCatalog:
    def __init__(self, school_name: str = "Escola Decia") -> None:
        self.templates = INSTITUTIONAL_MESSAGE_TEMPLATES
        self.school_name = (school_name or "Escola Decia").strip() or "Escola Decia"

    def build_message(
        self,
        parent_name: str,
        student_name: str,
        class_name_short: str,
        campaign_id: str,
        unique_key: str,
    ) -> tuple[str, str]:
        template = self._choose_template(campaign_id=campaign_id, unique_key=unique_key)
        message = template.text.format(
            parent_name=(parent_name or "Responsavel").strip(),
            student_name=(student_name or "Aluno(a)").strip(),
            class_name_short=self._normalize_class_name_short(class_name_short),
            school_name=self.school_name,
        )
        return template.template_id, message

    def _choose_template(self, campaign_id: str, unique_key: str) -> InstitutionalMessageTemplate:
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
