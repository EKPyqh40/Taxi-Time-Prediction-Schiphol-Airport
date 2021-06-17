# Taxi-Time-Prediction-Schiphol-Airport

This repository contains the code for the final article "Taxi Time Prediction with Classical and Auto Machine
Learning at Schiphol Airport" of the master thesis by Christophe Vakaet.

> Note: The files in this repository have been restructured and renamed. Most of the names and file paths within the code are therefore currently broken.

## Article

The article can be found [here](https://github.com/EKPyqh40/Taxi-Time-Prediction-Schiphol-Airport/blob/main/Thesis_AIAA.pdf).

### Abstract

Taxi time predictions are used by air traffic controllers to optimally release aircraft from the gate such that efficiency losses due to queuing are minimized, while runway capacity is maintained. More accurate taxi times can therefore result in improved airport surface operations and reduce air traffic controller workload. This article proposes a methodology to develop taxi time predictor and applies this methodology to Schiphol airport. The methodology combines novel data-driven predictors with different improvements and extensive performance evaluation. One such improvement involves using recent taxi time prediction errors to improve upcoming taxi time predictions. During evaluation, this article extends conventional analysis by analyzing different prediction horizons and performance metrics. Applying the methodology at Schiphol airport resulted in a predictor that increased the fraction of flights with a taxi time error of less than two minutes from 64.41% to 67.91% compared to the currently operational manual decision tree predictor.

## Code Overview

![Code Diagram](code_diagram.png "Code Diagram")

This repository is subdivided into six sections: data, data viewer, data preparation, modelling, results, and misc. The diagram above provides an overview of the links between the different sections, as well as the links between files within a section. Below each section is further detailed.

## Data

Due to intellectual property issues, most data has been redacted. However the data strcuture and description of the content are available.

## Data Viewer

![Radar Example](data_viewer/screenshot.png "Radar Example")

The data viewer is a Python program capable of visualizing astra data together with different maps and flt data. This program has been used for multiple purposes. First the program has been used to derive the runway, taxiway, and other polygons. Secondly the program has been used to verify the results from the data preparation phase.

### Controls

|keys| function|
|-|-|
|w, a, s, d| pan|
|q, e| zoom|
|r, f| increase/decrease speed (time)|
|z| reverse time |
|space| pause |
|number keys| toggle layers |

## Data Preparation

This section contains the code that prepares the different raw data into a single departures table for modelling.

## Modelling

This section contains the code for the different taxi time prediction models. The code additionally generates the performance results for the evaluation of the different models.

## Results

This section contains and additionally evaluates the results of the modelling phase.

## Misc

Two extra files have been added to the repository that contain some extra information:

* verification.ipynb
* queueing.ipynb

### verification.ipynb

Verfiication.ipynb contains queries to verify different aspects of the data preparation phase.

### queueing.ipynb

Queuing.ipynb contains analysis of the queueing data from the data preparation phase. The goal was to link potentially link taxi time, and taxi time prediction error with the calculated average queue times. The quick analysis performed in this file is however unable to find such a link. More investigation is required.

## To Do

* redo file paths and names
* verify code with new architecture (rerun)
* eliminate duplicate code (modelling)
