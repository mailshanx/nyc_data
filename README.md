NYC Taxi Tip Prediction Model
==============================

Machine Learning Model for Predicting Tips in NYC Taxis

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │    │
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
     


--------

### Instructions to run this tool

The repository is hosted at https://github.com/mailshanx/nyc_data

Clone the repository:

```
git clone https://github.com/mailshanx/nyc_data
cd nyc_data
```

Build the docker container:

```bash
./build_docker.sh
```

The Makefile presents a simple interface to manage and use this too.

First, we need to download and pre-process the data.

```bash
make data
```

We can verify that the tests run:

```bash
make tests
``` 


Next, we need to train and same the model. This will save the model data/processed folder.


```bash
make model
```

Let us evaluate our model:

```bash
make evaluate_model
```

We can now use this tool for batch predictions:

```bash

```
