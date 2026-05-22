<!-- back-to-top anchor -->
<a id="readme-top"></a>

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

<br />
<div align="center">
  <a href="https://github.com/eurocontrol/eurocontrolpy">
    <img src="https://upload.wikimedia.org/wikipedia/commons/b/b2/Eurocontrol_logo_2010.svg"
         alt="EUROCONTROL logo" width="80" height="80">
  </a>

  <h3 align="center">eurocontrolpy</h3>

  <p align="center">
    A Python client for EUROCONTROL PRISME&nbsp;/&nbsp;Network&nbsp;Manager (NM)
    trajectory and airspace-profile data stored in Oracle.
    <br />
    <a href="https://eurocontrol.github.io/eurocontrolpy/"><strong>Read the docs »</strong></a>
    &nbsp;·&nbsp;
    <a href="https://github.com/eurocontrol/eurocontrolpy/issues/new?labels=bug">Report Bug</a>
    &nbsp;·&nbsp;
    <a href="https://github.com/eurocontrol/eurocontrolpy/issues/new?labels=enhancement">Request Feature</a>
  </p>
</div>

---

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#built-with">Built With</a></li>
    <li><a href="#getting-started">Getting Started</a></li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#authors">Authors</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

---

## About The Project

`eurocontrolpy` provides an object-oriented Python interface to EUROCONTROL
PRISME / NM data.  It ships two interchangeable backends that expose the same
method surface:

| Class | Backend | Returns |
|---|---|---|
| `EUROCONTROLpy` | SQLAlchemy + python-oracledb | **pandas** DataFrames |
| `EUROCONTROLSpark` | PySpark + Oracle JDBC | **Spark** DataFrames |

Key capabilities:

- **Flights** — `flights_tidy`, `adrr_flights_tidy`
- **Airspace profiles** — `airspace_profiles_tidy`, `flights_airspace_profiles_tidy`
- **Point profiles & model trajectories** — `point_profiles_tidy`, `export_model_trajectory`
- **SO6 segment export** — `generate_so6`
- **Airspace geometries** — `acc_sf`, `ansp_sf`, `es_sf`, `fir_sf` (GeoDataFrames, SQLAlchemy only)
- **H3 hexagon polyfill** — `polyfill_h3` (pandas or Spark UDF)
- **IATA seasons** — `season_iata`, `iata_season_for_date`
- **OurAirports** — `airports_oa` (no DB required)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Built With

**Core**

* [![Python][Python-badge]][Python-url]
* [![Pandas][Pandas-badge]][Pandas-url]
* [![SQLAlchemy][SQLAlchemy-badge]][SQLAlchemy-url]
* [![python-oracledb][oracledb-badge]][oracledb-url]

**Optional**

* [![PySpark][Spark-badge]][Spark-url] — `EUROCONTROLSpark` backend
* [![GeoPandas][Geopandas-badge]][Geopandas-url] — airspace geometry methods
* [![H3][H3-badge]][H3-url] — hexagon polyfill
* [![GeoPy][GeoPy-badge]][GeoPy-url] — accurate geodesic distances

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Getting Started

### Prerequisites

* Python ≥ 3.8
* Access to the EUROCONTROL PRISME / NM Oracle database
* Credentials in environment variables:

```sh
export PRU_DEV_USR="your_user"
export PRU_DEV_PWD="your_password"
export PRU_DEV_DBNAME="hostname:port/servicename"
```

### Installation

```sh
pip install eurocontrolpy
```

Install optional extras as needed:

```sh
# Airspace geometries
pip install "eurocontrolpy[geo_shapes]"

# H3 polyfill
pip install "eurocontrolpy[h3]"

# PySpark backend
pip install "eurocontrolpy[spark]"

# Everything at once
pip install "eurocontrolpy[geo,geo_shapes,h3,spark]"
```

**From source:**

```sh
git clone https://github.com/eurocontrol/eurocontrolpy.git
cd eurocontrolpy
pip install -e ".[dev]"
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Usage

### pandas backend (recommended for notebooks)

```python
from eurocontrolpy import EUROCONTROLpy

ec = EUROCONTROLpy()   # reads PRU_DEV_* environment variables

# Flight list for one day
flights = ec.flights_tidy("2024-07-01", "2024-07-02")

# Point profiles
traj = ec.point_profiles_tidy("2024-07-01", "2024-07-02", profile="CTFM")

# SO6 segment export
so6 = ec.generate_so6(traj)

# FIR geometries + H3 polyfill at resolution 4
firs   = ec.fir_sf(cfmu_airac=517)
h3_df  = ec.polyfill_h3(firs, resolution=4)
```

### PySpark backend (large-scale pipelines)

```python
from eurocontrolpy import EUROCONTROLSpark, build_spark_oracle_session

spark, url, props = build_spark_oracle_session(jar_path="jars/ojdbc8.jar")
ec = EUROCONTROLSpark(spark=spark, url=url, props=props)

# All methods are identical — return type is Spark DataFrame instead of pandas
flights = ec.flights_tidy("2024-07-01", "2024-07-02")
flights.show(5)

# H3 polyfill runs as a distributed UDF on Spark workers
from eurocontrolpy import EUROCONTROLpy
ec_py = EUROCONTROLpy()
firs  = ec_py.fir_sf(517)                       # geometry fetched via pandas
spark_h3 = ec.polyfill_h3(firs, resolution=4)   # polyfill on Spark
spark_h3.show(5)

spark.stop()
```

Full API reference and more examples: **[eurocontrol.github.io/eurocontrolpy](https://eurocontrol.github.io/eurocontrolpy/)**

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Roadmap

- [x] Oracle connection via environment variables
- [x] Flights, airspace profiles and point profile queries
- [x] SO6 segment export
- [x] SQLAlchemy / pandas backend (`EUROCONTROLpy`)
- [x] ADRR flight list format
- [x] Airspace geometries with geopandas (`acc_sf`, `ansp_sf`, `es_sf`, `fir_sf`)
- [x] H3 hexagon polyfill (`polyfill_h3`) — pandas and Spark UDF variants
- [x] IATA season helpers
- [x] Sphinx documentation with GitHub Actions auto-deploy
- [ ] Async query execution

See the [open issues](https://github.com/eurocontrol/eurocontrolpy/issues) for proposed features and known issues.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Contributing

Contributions are welcome.  Please fork the repository and open a pull request.

1. Fork the project
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'Add my feature'`)
4. Push the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## License

Distributed under the MIT License.  See `LICENSE.txt` for details.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Authors

**Enrico Spinielli** — original concept, R package architecture, data model  
Aviation Intelligence Unit · [EUROCONTROL](https://www.eurocontrol.int)  
✉ [enrico.spinielli@eurocontrol.int](mailto:enrico.spinielli@eurocontrol.int)

**Quinten Goens** — Python port, SQLAlchemy backend, H3 polyfill, documentation  
Aviation Intelligence Unit · [EUROCONTROL](https://www.eurocontrol.int)  
✉ [quinten.goens@eurocontrol.int](mailto:quinten.goens@eurocontrol.int)

Project: [github.com/eurocontrol/eurocontrolpy](https://github.com/eurocontrol/eurocontrolpy)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Acknowledgments

* [Performance Review Unit (PRU)](https://www.eurocontrol.int/air-navigation-services-performance-review)
* [EUROCONTROL](https://www.eurocontrol.int)
* The [eurocontrol R package](https://github.com/eurocontrol/eurocontrol) from which this library was ported

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
[contributors-shield]: https://img.shields.io/github/contributors/eurocontrol/eurocontrolpy.svg?style=for-the-badge
[contributors-url]: https://github.com/eurocontrol/eurocontrolpy/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/eurocontrol/eurocontrolpy.svg?style=for-the-badge
[forks-url]: https://github.com/eurocontrol/eurocontrolpy/network/members
[stars-shield]: https://img.shields.io/github/stars/eurocontrol/eurocontrolpy.svg?style=for-the-badge
[stars-url]: https://github.com/eurocontrol/eurocontrolpy/stargazers
[issues-shield]: https://img.shields.io/github/issues/eurocontrol/eurocontrolpy.svg?style=for-the-badge
[issues-url]: https://github.com/eurocontrol/eurocontrolpy/issues
[license-shield]: https://img.shields.io/github/license/eurocontrol/eurocontrolpy.svg?style=for-the-badge
[license-url]: https://github.com/eurocontrol/eurocontrolpy/blob/master/LICENSE.txt

[Python-badge]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://www.python.org/
[Pandas-badge]: https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white
[Pandas-url]: https://pandas.pydata.org/
[SQLAlchemy-badge]: https://img.shields.io/badge/SQLAlchemy-444444?style=for-the-badge&logo=python&logoColor=white
[SQLAlchemy-url]: https://www.sqlalchemy.org/
[oracledb-badge]: https://img.shields.io/badge/python--oracledb-F80000?style=for-the-badge&logo=oracle&logoColor=white
[oracledb-url]: https://python-oracledb.readthedocs.io/
[Spark-badge]: https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white
[Spark-url]: https://spark.apache.org/docs/latest/api/python/
[Geopandas-badge]: https://img.shields.io/badge/GeoPandas-139C5A?style=for-the-badge&logo=python&logoColor=white
[Geopandas-url]: https://geopandas.org/
[H3-badge]: https://img.shields.io/badge/H3-1D6FA5?style=for-the-badge&logo=uber&logoColor=white
[H3-url]: https://h3geo.org/
[GeoPy-badge]: https://img.shields.io/badge/GeoPy-006699?style=for-the-badge&logo=python&logoColor=white
[GeoPy-url]: https://geopy.readthedocs.io/
