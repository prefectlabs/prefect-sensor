---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Prefect Sensor"
  text: "A modular sensor framework"
  tagline: Automate workflows from events in any system
  actions:
    - theme: brand
      text: Documentation
      link: /docs/about
    - theme: alt
      text: Sensors
      link: /docs/sensors
  image:
    light:
      src: /prefect_logomark.png
    dark:
      src: /prefect_logomark_light.png
    alt: Prefect

features:
  - title: Modular sensors
    icon: 🧩
    details: Drop-in classes for filesystem, SFTP, Kafka, and SQL. Each sensor is identified by its import path so you can mix built-ins with your own.
  - title: Prefect-native events
    icon: 📡
    details: Sensors emit through <code>prefect.events.emit_event</code>, so observations flow into Prefect Cloud's event stream and can trigger automations.
  - title: YAML-first configuration
    icon: ⚙️
    details: Configure sensors with a single YAML file. Run via the CLI, the published <code>ghcr.io/prefectlabs/prefect-sensor</code> container image, or embed <code>SensorManager</code> as a library.
---
