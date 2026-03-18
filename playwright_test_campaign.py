import argparse
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_TEMPLATE_PATH = Path("relatorios/Telefones_Teste.xlsx")
DEFAULT_CAMPAIGN_PATH = Path("relatorios/Campanha_TESTE.xlsx")

TEMPLATE_COLUMNS = [
    "enabled",
    "contact_label",
    "parent_name",
    "phone_sanitized",
    "student_name",
    "absence_days",
    "custom_message",
    "observacao_teste",
]

CAMPAIGN_COLUMNS = [
    "campaign_id",
    "data_criacao",
    "status_envio",
    "data_envio",
    "status_resposta",
    "observacao",
    "class_name",
    "student_name",
    "ra_raw",
    "ra_key",
    "parent_name",
    "phone_sanitized",
    "absence_days",
    "whatsapp_message",
    "contact_slot",
]


class PlaywrightTestCampaignBuilder:
    def create_template(self, output_path: Optional[Path] = None) -> Path:
        path = Path(output_path or DEFAULT_TEMPLATE_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)

        template_df = pd.DataFrame(
            [
                {
                    "enabled": "sim",
                    "contact_label": "Meu numero",
                    "parent_name": "Seu nome",
                    "phone_sanitized": "5514981324832",
                    "student_name": "ALUNO TESTE 1",
                    "absence_days": "9, 18",
                    "custom_message": "",
                    "observacao_teste": "Substitua pelo seu numero real para teste controlado.",
                },
                {
                    "enabled": "nao",
                    "contact_label": "Grupo privado",
                    "parent_name": "Grupo teste",
                    "phone_sanitized": "5514982307099",
                    "student_name": "ALUNO TESTE 2",
                    "absence_days": "11, 12",
                    "custom_message": "Ola! Esta e uma mensagem de teste da automacao. Nenhuma acao e necessaria.",
                    "observacao_teste": "Preencha apenas se quiser testar outro contato.",
                },
            ],
            columns=TEMPLATE_COLUMNS,
        )

        instructions_df = pd.DataFrame(
            [
                {
                    "instrucao": "Preencha somente numeros controlados por voce.",
                },
                {
                    "instrucao": "Use telefone no formato 55DDDNUMERO, apenas com digitos.",
                },
                {
                    "instrucao": "Marque enabled como sim apenas nas linhas que devem entrar no teste.",
                },
                {
                    "instrucao": "Se custom_message ficar vazio, o script cria uma mensagem segura de teste.",
                },
                {
                    "instrucao": "Esse arquivo e separado da Ready_To_Send real.",
                },
            ]
        )

        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            template_df.to_excel(writer, sheet_name="Contatos_Teste", index=False)
            instructions_df.to_excel(writer, sheet_name="Instrucoes", index=False)

        logger.info("Template de teste criado em %s", path)
        return path

    def build_campaign(
        self,
        template_path: Optional[Path] = None,
        output_path: Optional[Path] = None,
    ) -> Path:
        source_path = Path(template_path or DEFAULT_TEMPLATE_PATH)
        campaign_path = Path(output_path or DEFAULT_CAMPAIGN_PATH)

        if not source_path.exists():
            raise FileNotFoundError(
                f"Arquivo de contatos de teste nao encontrado: {source_path}",
            )

        logger.info("Lendo contatos de teste em %s", source_path)
        contacts_df = pd.read_excel(source_path, sheet_name="Contatos_Teste")

        missing_columns = sorted(set(TEMPLATE_COLUMNS).difference(contacts_df.columns))
        if missing_columns:
            raise KeyError(
                "Arquivo de teste sem colunas obrigatorias: " + ", ".join(missing_columns),
            )

        prepared_df = self._prepare_contacts(contacts_df)
        campaign_df = self._build_campaign_dataframe(prepared_df)

        campaign_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(campaign_path, engine="openpyxl") as writer:
            campaign_df.to_excel(writer, sheet_name="Campanha", index=False)

        logger.info(
            "Campanha de teste criada em %s com %s registro(s).",
            campaign_path,
            len(campaign_df),
        )
        return campaign_path

    def _prepare_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        prepared = df.copy()
        prepared["enabled"] = prepared["enabled"].apply(self._is_enabled)
        prepared = prepared[prepared["enabled"]].copy()
        prepared["phone_sanitized"] = prepared["phone_sanitized"].apply(self._normalize_phone)
        prepared = prepared[prepared["phone_sanitized"].ne("")].copy()

        if prepared.empty:
            raise ValueError(
                "Nenhum contato de teste habilitado com telefone valido foi encontrado.",
            )

        prepared = prepared.reset_index(drop=True)
        return prepared

    def _build_campaign_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        created_at = datetime.now()
        campaign_id = f"Campanha_TESTE_{created_at:%Y_%m_%d_%H%M}"

        rows = []
        for index, row in df.iterrows():
            student_name = self._safe_text(row["student_name"]) or f"ALUNO TESTE {index + 1}"
            parent_name = self._safe_text(row["parent_name"]) or "Contato teste"
            absence_days = self._safe_text(row["absence_days"]) or "9, 18"
            custom_message = self._safe_text(row["custom_message"])
            message = custom_message or self._default_test_message(
                parent_name=parent_name,
                student_name=student_name,
                absence_days=absence_days,
            )
            ra_key = f"TESTE-{index + 1:03d}"

            rows.append(
                {
                    "campaign_id": campaign_id,
                    "data_criacao": created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "status_envio": "pendente",
                    "data_envio": "",
                    "status_resposta": "sem_resposta",
                    "observacao": self._safe_text(row["observacao_teste"]),
                    "class_name": "TESTE_PLAYWRIGHT",
                    "student_name": student_name,
                    "ra_raw": ra_key,
                    "ra_key": ra_key,
                    "parent_name": parent_name,
                    "phone_sanitized": row["phone_sanitized"],
                    "absence_days": absence_days,
                    "whatsapp_message": message,
                    "contact_slot": "teste_controlado",
                }
            )

        return pd.DataFrame(rows, columns=CAMPAIGN_COLUMNS)

    @staticmethod
    def _default_test_message(
        parent_name: str,
        student_name: str,
        absence_days: str,
    ) -> str:
        return (
            f"Ola {parent_name}, esta e uma mensagem de teste controlado da automacao "
            f"da Escola Decia para validar o envio via Playwright. Referencia de teste: "
            f"{student_name}, dias {absence_days}. Nenhuma acao e necessaria."
        )

    @staticmethod
    def _is_enabled(value: object) -> bool:
        normalized = str(value or "").strip().lower()
        return normalized in {"1", "sim", "s", "true", "yes", "y"}

    @staticmethod
    def _normalize_phone(value: object) -> str:
        if pd.isna(value):
            return ""

        if isinstance(value, float) and value.is_integer():
            raw_value = str(int(value))
        else:
            raw_value = str(value).strip()
            if raw_value.endswith(".0"):
                raw_value = raw_value[:-2]

        digits = re.sub(r"\D", "", raw_value)
        if len(digits) not in {12, 13}:
            return ""
        return digits

    @staticmethod
    def _safe_text(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cria template e campanha controlada para testes do Playwright.",
    )
    parser.add_argument(
        "--create-template",
        action="store_true",
        help="Cria o arquivo relatorios/Telefones_Teste.xlsx.",
    )
    parser.add_argument(
        "--build-campaign",
        action="store_true",
        help="Gera relatorios/Campanha_TESTE.xlsx a partir da tabela de teste.",
    )
    parser.add_argument(
        "--template-path",
        help="Caminho do arquivo Telefones_Teste.xlsx.",
    )
    parser.add_argument(
        "--output-path",
        help="Caminho do arquivo Campanha_TESTE.xlsx.",
    )
    args = parser.parse_args()

    builder = PlaywrightTestCampaignBuilder()

    if not args.create_template and not args.build_campaign:
        parser.error("Use --create-template, --build-campaign ou ambos.")

    template_path = Path(args.template_path) if args.template_path else None
    output_path = Path(args.output_path) if args.output_path else None

    try:
        if args.create_template:
            builder.create_template(output_path=template_path)
        if args.build_campaign:
            builder.build_campaign(template_path=template_path, output_path=output_path)
    except Exception as exc:
        logger.exception("Falha na preparacao do teste Playwright: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
