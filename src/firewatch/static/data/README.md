# Límites provinciales

`spain-provinces.geojson` contiene los 52 límites provinciales de España
(50 provincias, Ceuta y Melilla) usados para colorear el mapa de pronóstico.

- Origen: [codeforgermany/click_that_hood](https://github.com/codeforgermany/click_that_hood),
  `public/data/spain-provinces.geojson`.
- Licencia: MIT (Copyright 2013-2021 Code for America).
- Transformación local: se conservan solo `cod_prov` (renombrado a
  `province_code`), `name` (renombrado a `province_name`) y la geometría. Los
  códigos son los de provincia del INE y coinciden con
  `firewatch.services.provinces.PROVINCES`.

Los polígonos son **límites administrativos**, no perímetros de incendio. No se
usan para estimar el área afectada.
