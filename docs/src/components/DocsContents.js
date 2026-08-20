import React from 'react';
import Link from '@docusaurus/Link';
import styles from './DocsContents.module.css';

const sections = [
  {
    title: 'Getting Started',
    to: '/getting-started/installation',
    description: 'Install undatum and complete a first conversion, validation, or SQL query.',
    links: [
      {label: 'Installation', to: '/getting-started/installation'},
      {label: 'Quick start', to: '/getting-started/quick-start'},
      {label: 'When to use', to: '/getting-started/when-to-use'},
      {label: 'Cookbook', to: '/getting-started/cookbook'},
      {label: 'Basic usage', to: '/getting-started/basic-usage'},
      {label: 'Performance', to: '/getting-started/performance'},
      {label: 'Troubleshooting', to: '/getting-started/troubleshooting'},
      {label: 'Best practices', to: '/getting-started/best-practices'},
    ],
  },
  {
    title: 'AI & agents',
    to: '/integrations/mcp',
    description: 'MCP server, agent tools, AI documentation, and LLM-oriented indexes.',
    links: [
      {label: 'MCP server', to: '/integrations/mcp'},
      {label: 'AI documentation', to: '/integrations/ai'},
      {label: 'Python SDK', to: '/integrations/sdk'},
      {label: 'Cookbook', to: '/getting-started/cookbook'},
      {label: 'ai command', to: '/commands/ai'},
      {label: 'doc command', to: '/commands/doc'},
    ],
  },
  {
    title: 'Use Cases',
    to: '/use-cases/format-conversion',
    description: 'End-to-end examples for conversion, pipelines, quality, SQL, and agents.',
    links: [
      {label: 'Format conversion', to: '/use-cases/format-conversion'},
      {label: 'Data pipelines', to: '/use-cases/data-pipelines'},
      {label: 'Quality and packaging', to: '/use-cases/quality-and-packaging'},
      {label: 'SQL and analytics', to: '/use-cases/sql-and-analytics'},
      {label: 'Agents and MCP', to: '/use-cases/agents-and-mcp'},
    ],
  },
  {
    title: 'CLI Reference',
    to: '/commands/',
    description: 'Command-by-command reference for convert, validate, SQL, databases, and more.',
    links: [
      {label: 'All commands', to: '/commands/'},
      {label: 'convert', to: '/commands/convert'},
      {label: 'validate', to: '/commands/validate'},
      {label: 'sql', to: '/commands/sql'},
      {label: 'stats / profile', to: '/commands/stats'},
      {label: 'pipeline', to: '/commands/pipeline'},
      {label: 'db', to: '/commands/db'},
      {label: 'api', to: '/commands/api'},
    ],
  },
  {
    title: 'Data File Formats',
    to: '/formats/',
    description: 'Honest capability matrix for 140+ formats via iterabledata.',
    links: [
      {label: 'Format support matrix', to: '/formats/'},
      {label: 'formats command', to: '/commands/formats'},
      {label: 'iterabledata formats', href: 'https://datenoio.github.io/iterabledata/formats/'},
    ],
  },
  {
    title: 'Development',
    to: '/development/contributing',
    description: 'Contributing, error-handling patterns, community, and license.',
    links: [
      {label: 'Contributing', to: '/development/contributing'},
      {label: 'Error handling patterns', to: '/development/error-handling'},
      {label: 'Community', to: '/development/community'},
      {label: 'Plugins', to: '/integrations/plugins'},
      {label: 'License', to: '/license'},
    ],
  },
];

function Section({title, to, description, links}) {
  return (
    <article className={styles.card}>
      <h3 className={styles.cardTitle}>
        <Link to={to}>{title}</Link>
      </h3>
      <p className={styles.cardDescription}>{description}</p>
      <ul className={styles.linkList}>
        {links.map((item) => (
          <li key={item.label}>
            {item.href ? (
              <a href={item.href}>{item.label}</a>
            ) : (
              <Link to={item.to}>{item.label}</Link>
            )}
          </li>
        ))}
      </ul>
    </article>
  );
}

export default function DocsContents() {
  return (
    <section className={styles.contents}>
      <div className="container">
        <h2 className={styles.heading}>Documentation contents</h2>
        <p className={styles.intro}>
          Start with a section below, or use the sidebar from any page. The CLI
          entry points are <code>undatum</code> and the shorter <code>data</code> alias.
        </p>
        <div className={styles.grid}>
          {sections.map((section) => (
            <Section key={section.title} {...section} />
          ))}
        </div>
      </div>
    </section>
  );
}
