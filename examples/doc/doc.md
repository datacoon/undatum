# Dataset Documentation

## Metadata
- **filename**: data.jsonl
- **file_size**: 19622512
- **file_size_human**: 18.71 MB
- **file_type**: jsonl
- **compression**: raw
- **encoding**: utf-8
- **title**: Данные о медицинских препаратах
- **keywords**: медицинские препараты, фармацевтика, лекарственные средства, здоровье
- **geographic_coverage**:
  - countries: -
  - regions: -
  - coordinates_present: No
- **temporal_coverage**:
  - start: 2021-09-16T18:58:09.699165
  - end: 2024-12-09T10:50:28.789158
  - granularity: день
- **languages**: code: ru; confidence: 1.0
- **data_theme**:
  - label: HEAL
  - uri: http://publications.europa.eu/resource/authority/data-theme/HEAL
- **metadata_confidence**:
  - title: 1.0
  - keywords: 1.0
  - geographic_coverage: 0.5
  - temporal_coverage: 1.0
  - languages: 1.0
  - data_theme: 1.0
- **metadata_evidence**:
  - title: Данные о медицинских препаратах
  - keywords: Содержит ключевые слова, связанные с медициной и фармацевтикой.
  - geographic_coverage: Нет конкретных стран или регионов.
  - temporal_coverage: Даты создания и изменения данных указаны.
  - languages: Данные представлены на русском языке.
  - data_theme: Тематика данных относится к здравоохранению.

## Summary
| Metric        |   Value |
|---------------|---------|
| total_tables  |       1 |
| total_records |   14550 |

## Schema
### Table: data.jsonl
- Records: 14550
- Columns: 29
- Flat: No

Summary:
Данный набор данных содержит информацию о медицинских препаратах, включая уникальные идентификаторы, названия, формы выпуска, дозировки, классификации, а также даты создания и обновления записей. Каждая запись представляет собой отдельный препарат с характеристиками, такими как активные вещества, форма, количество и назначение. Набор данных может использоваться для анализа и мониторинга лекарственных средств.

| Field                     | Type    | Array   | Description                                               |
|---------------------------|---------|---------|-----------------------------------------------------------|
| smnn_gid                  | UUID    | No      | Уникальный идентификатор смнн.                            |
| std_mnn_gid               | UUID    | No      | Уникальный идентификатор стандартного МНН.                |
| std_mnn_name              | VARCHAR | No      | Название стандартного МНН.                                |
| norm_mnn                  | VARCHAR | Yes     | Нормативный МНН.                                          |
| std_lf_gid                | UUID    | No      | Уникальный идентификатор стандартной лекарственной формы. |
| std_lf_name               | VARCHAR | No      | Название стандартной лекарственной формы.                 |
| norm_lf                   | VARCHAR | Yes     | Нормативная лекарственная форма.                          |
| std_dosage                | VARCHAR | No      | Стандартная дозировка.                                    |
| norm_dosage               | VARCHAR | Yes     | Нормативная дозировка.                                    |
| smnn_pe_gid               | UUID    | No      | Уникальный идентификатор смнн в рамках ПЭ.                |
| smnn_pe                   | VARCHAR | No      | Название смнн в рамках ПЭ.                                |
| smnn_code                 | VARCHAR | No      | Код смнн.                                                 |
| klp_all_list              | VARCHAR | No      | Список всех КЛП, связанных с смнн.                        |
| klp_active_list           | VARCHAR | No      | Список активных КЛП, связанных с смнн.                    |
| smnn_active_klp_count     | BIGINT  | No      | Количество активных КЛП для данного смнн.                 |
| smnn_all_klp_count        | BIGINT  | No      | Общее количество КЛП для данного смнн.                    |
| smnn_ref_price_list       | VARCHAR | Yes     | Список референсных цен для смнн.                          |
| is_znvlp                  | BOOLEAN | No      | Флаг, указывающий, является ли смнн ЗНВЛП.                |
| is_narcotic               | BOOLEAN | No      | Флаг, указывающий, является ли смнн наркотическим.        |
| smnn_is_active            | BOOLEAN | No      | Флаг, указывающий, активен ли смнн.                       |
| smnn_is_invalid           | BOOLEAN | No      | Флаг, указывающий, является ли смнн недействительным.     |
| smnn_date_invalid         | DATE    | No      | Дата, когда смнн стал недействительным.                   |
| okpd2                     | VARCHAR | No      | Код ОКПД2 для смнн.                                       |
| ath                       | VARCHAR | Yes     | Атрибуты терапевтической группы.                          |
| ftg                       | VARCHAR | Yes     | Фармакотерапевтическая группа.                            |
| interchangeability_groups | BIGINT  | Yes     | Группы взаимозаменяемости для смнн.                       |
| smnn_pe_note              | VARCHAR | No      | Примечание к смнн в рамках ПЭ.                            |
| smnn_date_create          | VARCHAR | No      | Дата создания записи смнн.                                |
| smnn_date_change          | VARCHAR | No      | Дата последнего изменения записи смнн.                    |

## Statistics
| Field                     |   Unique |   Total |   Unique % |
|---------------------------|----------|---------|------------|
| smnn_gid                  |     9728 |   10000 |      97.28 |
| std_mnn_gid               |     4887 |   10000 |      48.87 |
| std_mnn_name              |     3075 |   10000 |      30.75 |
| norm_mnn                  |      466 |   10000 |       4.66 |
| std_lf_gid                |      561 |   10000 |       5.61 |
| std_lf_name               |      438 |   10000 |       4.38 |
| norm_lf                   |      530 |   10000 |       5.3  |
| std_dosage                |     2781 |   10000 |      27.81 |
| norm_dosage               |     1487 |   10000 |      14.87 |
| smnn_pe_gid               |       13 |   10000 |       0.13 |
| smnn_pe                   |       16 |   10000 |       0.16 |
| smnn_code                 |     9511 |   10000 |      95.11 |
| klp_all_list              |     5785 |   10000 |      57.85 |
| klp_active_list           |     4166 |   10000 |      41.66 |
| smnn_active_klp_count     |      488 |   10000 |       4.88 |
| smnn_all_klp_count        |      742 |   10000 |       7.42 |
| smnn_ref_price_list       |        1 |   10000 |       0.01 |
| is_znvlp                  |        2 |   10000 |       0.02 |
| is_narcotic               |        2 |   10000 |       0.02 |
| smnn_is_active            |        2 |   10000 |       0.02 |
| smnn_is_invalid           |        2 |   10000 |       0.02 |
| smnn_date_invalid         |        6 |   10000 |       0.06 |
| okpd2                     |      112 |   10000 |       1.12 |
| ath                       |     2994 |   10000 |      29.94 |
| ftg                       |     1700 |   10000 |      17    |
| interchangeability_groups |     1059 |   10000 |      10.59 |
| smnn_pe_note              |      140 |   10000 |       1.4  |
| smnn_date_create          |      148 |   10000 |       1.48 |
| smnn_date_change          |      817 |   10000 |       8.17 |

## Samples
```json
[
  {
    "smnn_gid": "00591e7c-1707-11ec-b64c-b3eabe615e17",
    "std_mnn_gid": "f51864d0-6638-4c60-b445-965ee316fae5",
    "std_mnn_name": "ФЕБУКСОСТАТ",
    "norm_mnn": null,
    "std_lf_gid": "25779a9a-0ac2-4f59-9557-2965c8e715de",
    "std_lf_name": "Капсулы",
    "norm_lf": null,
    "std_dosage": "80 мг",
    "norm_dosage": null,
    "smnn_pe_gid": "76b8a1df-ff1c-4363-a83c-bbebc0b04167",
    "smnn_pe": "шт.",
    "smnn_code": "21.20.10.226-000001-1-00008-0000000000000",
    "klp_all_list": "Подагрель",
    "klp_active_list": "Подагрель",
    "smnn_active_klp_count": 4,
    "smnn_all_klp_count": 4,
    "smnn_ref_price_list": [
      null,
      null,
      null
    ],
    "is_znvlp": false,
    "is_narcotic": false,
    "smnn_is_active": true,
    "smnn_is_invalid": false,
    "smnn_date_invalid": null,
    "okpd2": "21.20.10.226",
    "ath": [
      "M04AA03 - Фебуксостат"
    ],
    "ftg": [
      "противоподагрическое средство - ксантиноксидазы ингибитор"
    ],
    "interchangeability_groups": null,
    "smnn_pe_note": "капсула",
    "smnn_date_create": "2021-09-16T18:58:09.699165",
    "smnn_date_change": "2021-09-16T19:08:42.981362"
  },
  {
    "smnn_gid": "00592282-1707-11ec-b64c-67a91b8a3b59",
    "std_mnn_gid": "bdf5198c-1706-11ec-badd-9f34b38d96fc",
    "std_mnn_name": "ДОЛУТЕГРАВИР+РИЛПИВИРИН",
    "norm_mnn": null,
    "std_lf_gid": "12f50236-bf5b-11e9-a17d-17d28df99c7e",
    "std_lf_name": "Таблетки, покрытые оболочкой",
    "norm_lf": [
      "Таблетки покрытые пленочной оболочкой"
    ],
    "std_dosage": "50 мг+25 мг",
    "norm_dosage": [
      "50 мг + 25 мг"
    ],
    "smnn_pe_gid": "76b8a1df-ff1c-4363-a83c-bbebc0b04167",
    "smnn_pe": "шт.",
    "smnn_code": "21.20.10.194-000111-1-00001-0000000000000",
    "klp_all_list": "Джулука",
    "klp_active_list": "Джулука",
    "smnn_active_klp_count": 1,
    "smnn_all_klp_count": 2,
    "smnn_ref_price_list": [
      null,
      null,
      null
    ],
    "is_znvlp": false,
    "is_narcotic": false,
    "smnn_is_active": true,
    "smnn_is_invalid": false,
    "smnn_date_invalid": null,
    "okpd2": "21.20.10.194",
    "ath": [
      "J05AR21 - Долутегравир+Рилпивирин"
    ],
    "ftg": [
      "противовирусное [ВИЧ] средство",
      "противовирусные средства системного действия; противовирусные средства прямого действия; комбинации противовирусных средств для лечения ВИЧ-инфекций"
    ],
    "interchangeability_groups": null,
    "smnn_pe_note": "таблетка",
    "smnn_date_create": "2021-09-16T18:58:09.699165",
    "smnn_date_change": "2022-07-14T13:07:24.883854"
  },
  {
    "smnn_gid": "005923fe-1707-11ec-b64c-e7b754d2657a",
    "std_mnn_gid": "0a2b8f6c-bf5b-11e9-a145-a36e91109ae4",
    "std_mnn_name": "ВАКЦИНА ДЛЯ ПРОФИЛАКТИКИ РОТАВИРУСНОЙ ИНФЕКЦИИ, ПЕНТАВАЛЕНТНАЯ, ЖИВАЯ",
    "norm_mnn": [
      "ВАКЦИНА ДЛЯ ПРОФИЛАКТИКИ РОТАВИРУСНОЙ ИНФЕКЦИИ,ПЕНТАВАЛЕНТНАЯ,ЖИВАЯ"
    ],
    "std_lf_gid": "f8adcfca-c01b-4485-9353-3a9134461ed8",
    "std_lf_name": "Лиофилизат для приготовления раствора для приема внутрь",
    "norm_lf": null,
    "std_dosage": "2.5 мл/доза",
    "norm_dosage": [
      "2,5 мл/доза"
    ],
    "smnn_pe_gid": "0c26abe4-bf5b-11e9-b5ac-db2c1527b843",
    "smnn_pe": "доз(а)",
    "smnn_code": "21.20.21.120-000097-1-00072-0000000000000",
    "klp_all_list": "Рота-V-Эйд",
    "klp_active_list": "Рота-V-Эйд",
    "smnn_active_klp_count": 3,
    "smnn_all_klp_count": 10,
    "smnn_ref_price_list": [
      null,
      null,
      null
    ],
    "is_znvlp": true,
    "is_narcotic": false,
    "smnn_is_active": true,
    "smnn_is_invalid": false,
    "smnn_date_invalid": null,
    "okpd2": "21.20.21.120",
    "ath": [
      "J07BH02 - Ротавирус пятивалентный живой"
    ],
    "ftg": [
      "МИБП - вакцина"
    ],
    "interchangeability_groups": [
      4776
    ],
    "smnn_pe_note": null,
    "smnn_date_create": "2021-09-16T18:58:09.699165",
    "smnn_date_change": "2024-12-09T10:50:28.789158"
  }
]
```
