import {defineConfig} from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
	title: "Prefect Sensor",
	description: "A modular sensor framework for Prefect: automate workflows from events in any system",
	base: '/prefect-sensor',
	themeConfig: {
		// https://vitepress.dev/reference/default-theme-config
		nav: [
			{
				text: 'Home', 
				link: '/'
			},
			{
				text: 'Docs', 
				link: '/docs/about'
			}
		],

		sidebar: [
			{
				text: "Overview",
				items: [
					{
						text: "About",
						link: '/docs/about'
					},
					{
						text: "Getting Started",
						link: "/docs/getting-started"
					},
					{
						text: "Docker",
						link: "/docs/docker"
					}
				]
			},
			{
				text: "Sensors",
				link: "/docs/sensors",
				items: [
					{
						text: "File System",
						link: "/docs/sensors/filesystem"
					},
					{
						text: "SFTP",
						link: "/docs/sensors/sftp"
					},
					{
						text: "Kafka",
						link: "/docs/sensors/kafka"
					},
					{
						text: "SQL",
						link: "/docs/sensors/sql"
					}
				]
			}
		],

		socialLinks: [
			{
				icon: 'github',
				link: 'https://github.com/prefectlabs/prefect-sensor'
			},
			{
				icon: 'slack',
				link: 'https://prefect.io/slack'
			}
		]
	}
})
