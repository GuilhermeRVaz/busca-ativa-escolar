import argparse
from pathlib import Path

import pandas as pd


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _append_observation(current: object, extra: str) -> str:
    existing = _safe_text(current)
    return extra if not existing else f"{existing} | {extra}"


def sync_progress(
    source_campaign_path: Path,
    target_campaign_path: Path,
    output_path: Path,
) -> tuple[int, int]:
    source_df = pd.read_excel(source_campaign_path, sheet_name="Campanha")
    target_df = pd.read_excel(target_campaign_path, sheet_name="Campanha")

    for df in (source_df, target_df):
        df["ra_key"] = df["ra_key"].fillna("").astype(str).str.strip()
        df["status_envio"] = df["status_envio"].fillna("").astype(str).str.strip().str.lower()
        df["status_resposta"] = df["status_resposta"].fillna("").astype(str).str.strip()
        df["data_envio"] = df["data_envio"].fillna("").astype(str).str.strip()
        df["observacao"] = df["observacao"].fillna("").astype(str).str.strip()

    sent_source = source_df[source_df["status_envio"].eq("enviado")].copy()
    sent_source = sent_source[sent_source["ra_key"].ne("")].copy()
    sent_source = sent_source.sort_values(["data_envio", "student_name"], kind="stable")
    sent_by_ra = sent_source.drop_duplicates(subset=["ra_key"], keep="first").set_index("ra_key")

    migrated = 0
    for index, row in target_df.iterrows():
        ra_key = _safe_text(row.get("ra_key"))
        if not ra_key or ra_key not in sent_by_ra.index:
            continue
        source_row = sent_by_ra.loc[ra_key]
        target_df.at[index, "status_envio"] = "enviado"
        target_df.at[index, "data_envio"] = _safe_text(source_row.get("data_envio"))
        if _safe_text(source_row.get("status_resposta")):
            target_df.at[index, "status_resposta"] = _safe_text(source_row.get("status_resposta"))
        target_df.at[index, "observacao"] = _append_observation(
            target_df.at[index, "observacao"],
            (
                "Progresso migrado da campanha institucional anterior "
                f"({source_campaign_path.name}) para evitar reenvio."
            ),
        )
        migrated += 1

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        target_df.to_excel(writer, sheet_name="Campanha", index=False)

    return migrated, len(target_df)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migra os envios ja realizados de uma campanha institucional antiga para a campanha fallback.",
    )
    parser.add_argument("--source-campaign", required=True, help="Campanha institucional antiga.")
    parser.add_argument("--target-campaign", required=True, help="Campanha institucional fallback.")
    parser.add_argument("--output", required=True, help="Arquivo de saida para a campanha retomada.")
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    migrated, total = sync_progress(
        source_campaign_path=Path(args.source_campaign),
        target_campaign_path=Path(args.target_campaign),
        output_path=Path(args.output),
    )
    print(f"Campanha retomada salva em {args.output}")
    print(f"Alunos migrados como enviados: {migrated}")
    print(f"Total de linhas na campanha retomada: {total}")


if __name__ == "__main__":
    main()
