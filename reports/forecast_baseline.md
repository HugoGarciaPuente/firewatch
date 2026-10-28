# Baseline de predicción de incendios a 24 h

Unidad: **provincia-día**. Etiqueta: incendio confirmado EFFIS de **≥ 10 ha** iniciado ese día en la provincia.

## Dataset

| Split | Fechas | Filas | Positivos | Tasa |
|---|---|---:|---:|---:|
| train | 2017-01-04 .. 2024-12-31 | 151,788 | 2,080 | 1.37 % |
| validation | 2025-01-01 .. 2025-12-31 | 18,980 | 444 | 2.34 % |
| test | 2026-01-01 .. 2026-10-25 | 15,496 | 494 | 3.19 % |

HGB: 250 iteraciones, `learning_rate=0.03`, `max_leaf_nodes=15`, `min_samples_leaf=50`, `l2_regularization=1.0`. La calibración isotónica se ajusta en validación 2025, nunca en test.

## Test 2026 (todas las estaciones)

| Modelo | PR-AUC | ROC-AUC | Brier | ECE | P@1/día | P@3/día | P@5/día | P@10/día |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Climatología | 0.1616 | 0.8052 | 0.0290 | 0.0161 | 0.1711 | 0.1555 | 0.1262 | 0.0926 |
| Regresión logística | 0.1850 | 0.8507 | 0.0286 | 0.0139 | 0.1477 | 0.1286 | 0.1148 | 0.0940 |
| Logística calibrada | 0.1749 | 0.8502 | 0.0284 | 0.0043 | 0.1309 | 0.1309 | 0.1195 | 0.0943 |
| HistGradientBoosting | 0.2667 | 0.8684 | 0.0273 | 0.0144 | 0.1611 | 0.1566 | 0.1416 | 0.0990 |
| HGB calibrado | 0.2443 | 0.8666 | 0.0271 | 0.0069 | 0.1544 | 0.1566 | 0.1369 | 0.1020 |

## Temporada mayo-octubre

| Modelo | PR-AUC | Brier | ECE | P@5/día |
|---|---:|---:|---:|---:|
| Climatología | 0.0792 | 0.0351 | 0.0226 | 0.0876 |
| Regresión logística | 0.1107 | 0.0348 | 0.0200 | 0.0921 |
| Logística calibrada | 0.1066 | 0.0350 | 0.0106 | 0.0978 |
| HistGradientBoosting | 0.1225 | 0.0343 | 0.0189 | 0.1101 |
| HGB calibrado | 0.1141 | 0.0343 | 0.0094 | 0.1000 |

## PR-AUC por año (test)

| Modelo | 2025 (validación) | 2026 (test) |
|---|---:|---:|
| Climatología | 0.1479 | 0.1616 |
| Regresión logística | 0.2125 | 0.1850 |
| HistGradientBoosting | 0.2882 | 0.2667 |

## Lift frente a climatología (test)

| Modelo | PR-AUC | P@5/día |
|---|---:|---:|
| Regresión logística | 1.145 | 0.910 |
| HistGradientBoosting | 1.651 | 1.122 |

## Variables más importantes (HGB, validación 2025)

| Variable | Importancia |
|---|---:|
| `latitude` | 0.1634 |
| `longitude` | 0.0942 |
| `precip_sum_7` | 0.0700 |
| `vpd_kpa` | 0.0395 |
| `month` | 0.0369 |
| `days_since_rain` | 0.0364 |
| `rh_min_lag1` | 0.0291 |
| `doy_cos` | 0.0280 |
| `precip_lag1` | 0.0206 |
| `rh_min` | 0.0188 |

Latitud y longitud dominan porque codifican la geografía provincial (qué provincias arden más), no porque el modelo sea puramente espacial: después aparecen la lluvia acumulada, el VPD, el mes y los días sin lluvia.

## Notas

- La meteorología del día D se usa como proxy de la previsión disponible la víspera; la evaluación histórica usa reanálisis ERA5, no una previsión archivada. El resultado es optimista si la previsión real es peor.
- Los lags, la lluvia acumulada y `days_since_rain` usan solo días anteriores al objetivo.
- El test 2026 no se ha usado para ajustar hiperparámetros ni calibración.
- La tasa positiva de test (3.19 %) no es la de entrenamiento (1.37 %): 2026 fue un año de más incendios y los valores absolutos no se trasladan a un año medio.
