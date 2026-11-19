# SRO Ranking Scraper

Pipeline de extração e persistência histórica de rankings de jogadores e guildas de servidores privados de **Silkroad Online**. Os dados são armazenados em formato de *snapshots* temporais no **PostgreSQL**, prontos para consumo analítico no **Power BI** ou **Streamlit**.

## Arquitetura

```text
[Sites de Ranking] ──> [HTTPX / Playwright] ──> [Parsers (Strategy)]
                                                        │
[Power BI / Streamlit] <── [Views SQL] <── [PostgreSQL (Snapshots)]
```

## Fontes de Dados / Alvos

- **Chronicle SRO** (Base inicial de pesquisa):
  - **Ranking URL:** [https://www.play-chronicle.com/ranking/index.html](https://www.play-chronicle.com/ranking/index.html)
  - **Status:** Em fase de mapeamento e análise de estrutura

- **Electus Online**:
  - **URL / Referência:** [https://electus.online/en/ranking](https://electus.online/en/ranking)
  - **Status:** Em fase de mapeamento e análise de estrutura

- **Play Golden (Golden Online)**:
  - **Ranking URL:** [https://play-golden.online/ranking.html](https://play-golden.online/ranking.html)
  - **Status:** Em fase de mapeamento e análise de estrutura
