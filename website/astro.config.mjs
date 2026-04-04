// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// When deploying to GitHub Pages the CI sets SITE and BASE_PATH via
// the configure-pages action so internal links resolve correctly.
// In local dev both are unset and the defaults below are used.
const site = process.env.SITE ?? 'https://strata-db.github.io';
const base = process.env.BASE_PATH ?? '/strata';

// https://astro.build/config
export default defineConfig({
	site,
	base,
	integrations: [
		starlight({
			title: 'Strata',
			description: 'An embeddable, S3-durable key-value store for Go.',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/strata-db/strata' },
			],
			customCss: ['./src/styles/custom.css'],
			components: {
				Hero: './src/components/Hero.astro',
			},
			sidebar: [
				{ label: 'Getting Started', slug: 'getting-started' },
				{
					label: 'Guides',
					items: [
						{ label: 'API Reference', slug: 'api' },
						{ label: 'Configuration', slug: 'configuration' },
						{ label: 'Operations', slug: 'operations' },
						{ label: 'Security', slug: 'security' },
						{ label: 'Recipes', slug: 'recipes' },
					],
				},
				{
					label: 'Deployment',
					items: [
						{ label: 'Kubernetes', slug: 'deployment/kubernetes' },
						{ label: 'Docker Compose', slug: 'deployment/docker-compose' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Architecture', slug: 'architecture' },
						{ label: 'Benchmarks', slug: 'benchmarks' },
					],
				},
				{ label: 'Migrating from etcd', slug: 'etcd-migration' },
				{ label: 'Troubleshooting', slug: 'troubleshooting' },
				{ label: 'FAQ', slug: 'faq' },
			],
			head: [
				{
					tag: 'meta',
					attrs: {
						property: 'og:description',
						content: 'An embeddable, S3-durable key-value store for Go with etcd v3 compatibility.',
					},
				},
			],
		}),
	],
});
