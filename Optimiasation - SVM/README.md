# Optimal Separating Hyperplane Project

## Introduction

This project demonstrates the methodology for identifying an optimal separating hyperplane within a dataset, leveraging Mixed Integer Linear Programming (MiP) in Python. It examines various models, focusing on their ability to segregate datasets with varying levels of complexity and separability.

## Project Structure

- **optimal_hyperplane.py**: The main script, encompassing all necessary implementations for generating data, defining models, and visualizing the outcomes.

## Models

The project explores the following models, each tailored to handle the data's linear separability with distinct approaches:

- **No Loss Function**: Targets linearly separable datasets without incorporating a loss function, aiming for a straightforward separation.
- **Hinge Loss**: Introduces error variables to accommodate points within the margin, optimizing the separation by penalizing margin violations.
- **Ramp Loss**: Advances the concept of hinge loss by capping the loss value, thereby managing misclassifications and margin violations with a focus on maximizing class separation and enhancing model robustness against outliers.

This README briefly outlines the project's objectives, utilized technologies, and organizational structure. For detailed insights or further inquiries, please refer to the `optimal_hyperplane.py` script.
