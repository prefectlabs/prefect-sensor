---
title: About
---

# About

### Why Prefect Sensor?

Prefect Sensor provides a modular framework for building sensors that watch external systems and emit Prefect events. It is designed to be flexible and extensible, allowing you to create custom sensors for any system or event.

In Prefect Cloud, webhooks act as a way to bridge third-party events into Prefect's event stream. However, not all systems support webhooks, and some may require polling or other mechanisms to detect events. Prefect Sensor fills this gap by providing a framework for building sensors that can watch any system and emit events into Prefect.

Prefect Sensor is designed with the philosophy of how our build/push/pull steps in a `prefect.yaml` file work. Each sensor is a Python class that implements a `watch` method that yields events. Sensors can be configured with parameters and secrets, and can be run as standalone processes or as part of a larger Prefect workflow.