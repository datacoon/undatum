// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer').themes.github;
const darkCodeTheme = require('prism-react-renderer').themes.dracula;

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'undatum',
  tagline: 'Command-line tool for data processing and analysis',
  favicon: 'img/favicon.svg',

  url: 'https://datenoio.github.io',
  baseUrl: '/undatum/',

  organizationName: 'datenoio',
  projectName: 'undatum',
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/datenoio/undatum/edit/master/docs/docs/',
          routeBasePath: '/',
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/logo.svg',
      navbar: {
        title: 'undatum',
        logo: {
          alt: 'undatum logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            to: '/',
            label: 'Contents',
            position: 'left',
            activeBaseRegex: '^/undatum/?$',
          },
          {
            type: 'docSidebar',
            sidebarId: 'docs',
            position: 'left',
            label: 'Docs',
          },
          {
            to: '/getting-started/cookbook',
            label: 'Cookbook',
            position: 'left',
          },
          {
            href: 'https://datenoio.github.io/undatum/llms.txt',
            label: 'llms.txt',
            position: 'right',
          },
          {
            href: 'https://github.com/datenoio/undatum',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Contents',
                to: '/',
              },
              {
                label: 'Getting Started',
                to: '/getting-started/installation',
              },
              {
                label: 'CLI Reference',
                to: '/commands/',
              },
              {
                label: 'Formats',
                to: '/formats/',
              },
              {
                label: 'Cookbook',
                to: '/getting-started/cookbook',
              },
            ],
          },
          {
            title: 'For coding agents',
            items: [
              {
                label: 'llms.txt',
                href: 'https://datenoio.github.io/undatum/llms.txt',
              },
              {
                label: 'MCP server',
                to: '/integrations/mcp',
              },
              {
                label: 'Python SDK',
                to: '/integrations/sdk',
              },
              {
                label: 'AI documentation',
                to: '/integrations/ai',
              },
            ],
          },
          {
            title: 'Project',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/datenoio/undatum',
              },
              {
                label: 'PyPI',
                href: 'https://pypi.org/project/undatum/',
              },
              {
                label: 'Changelog',
                href: 'https://github.com/datenoio/undatum/blob/master/CHANGELOG.md',
              },
              {
                label: 'License',
                to: '/license',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Ivan Begtin and contributors. undatum is MIT licensed.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: ['python', 'bash', 'yaml', 'json'],
      },
    }),
};

module.exports = config;
