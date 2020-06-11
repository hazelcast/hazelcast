/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config for all the possible
// site configuration options.

const CWD = process.cwd();
const isLocalhost = Boolean(
    process.env.NODE_ENV === 'development'
);

const siteConfig = {
    title: 'Hazelcast Jet', // Title for your website.
    tagline: 'Open-Source Distributed Stream Processing',
    url: 'https://jet-start.sh', // Your website URL
    baseUrl: '/', // Base URL for your project */
    // For github.io type URLs, you would set the url and baseUrl like:
    //   url: 'https://facebook.github.io',
    //   baseUrl: '/test-site/',
    cname: 'jet-start.sh',
    // Used for publishing and more
    projectName: 'hazelcast-jet',
    organizationName: 'hazelcast',
    // For top-level user or org sites, the organization is still the same.
    // e.g., for the https://JoelMarcey.github.io site, it would be set like...
    //   organizationName: 'JoelMarcey'

    // For no header links in the top nav bar -> headerLinks: [],
    headerLinks: [
        {doc: 'get-started/intro', label: 'Docs'},
        // {doc: 'connectors/imap', label: 'Connectors'},
        {page: 'download', label: "Download"},
        {page: 'demos', label: "Demos"},
        // {href: `/javadoc/${latestVersion}`, label: "Javadoc"},
        {href: 'https://github.com/hazelcast/hazelcast-jet', label: "GitHub"},
        {href: 'https://gitter.im/hazelcast/hazelcast-jet', label: "Contact"},
        {blog: true, label: 'Blog'},
        {search: true},
    ],
    disableHeaderTitle: true,
    noIndex: false, // do not crawl website

    // If you have users set above, you add it here:
    // users,

    /* path to images for header/footer */
    headerIcon: 'img/logo-dark.svg',
    footerIcon: 'img/logo-light.svg',
    favicon: 'img/favicon.png',

    /* Colors for website */
    colors: {
        primaryColor: '#2d7de4',
        secondaryColor: '#e5e5e5',
    },

    stylesheets: [
        'https://fonts.googleapis.com/css?family=Ubuntu:300,400,500,600',
        'https://fonts.googleapis.com/css?family=Source+Sans+Pro:300,400,500,600,700,800'
    ],

    /* Custom fonts for website */
    fonts: {
        myFont: [
            'Ubuntu',
            'Source Sans Pro'
        ],
    },

    // This copyright info is used in /core/Footer.js and blog RSS/Atom feeds.
    copyright: `Copyright © ${new Date().getFullYear()} Hazelcast Inc.`,

    highlight: {
        // Highlight.js theme to use for syntax highlighting in code blocks.
        theme: 'default',
    },

    usePrism: ['java'],

    // Add custom scripts here that would be placed in <script> tags.
    scripts: ['https://buttons.github.io/buttons.js', 
    'https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.0/clipboard.min.js',
    '/js/code-block-buttons.js',
    ],

    separateCss: ['static/javadoc'],

    algolia: {
        apiKey: '79d1e4941621b9fd761d279d4d19ed69',
        indexName: 'hazelcast-jet',
        algoliaOptions: {
            facetFilters: [ "language:LANGUAGE", "version:VERSION" ]
        }
    },

    // On page navigation for the current documentation page.
    onPageNav: 'separate',
    // No .html extensions for paths.
    cleanUrl: true,

    editUrl: 'https://github.com/hazelcast/hazelcast-jet/edit/master/site/docs/',

    // Open Graph and Twitter card images.
    ogImage: 'img/Hazelcast-Jet-Logo-Blue_Dark.jpg',
    twitterImage: 'img/Hazelcast-Jet-Logo-Blue_Dark.jpg',

    // For sites with a sizable amount of content, set collapsible to true.
    // Expand/collapse the links and subcategories under categories.
    docsSideNavCollapsible: true,
    blogSidebarCount: 'ALL',
    blogSidebarTitle: { default: 'Recent posts', all: 'All posts' },
    // Show documentation's last contributor's name.
    // enableUpdateBy: true,

    // Show documentation's last update time.
    // enableUpdateTime: true,

    // You may provide arbitrary config keys to be used as needed by your
    // template. For example, if you need your repo's URL...
    repoUrl: 'https://github.com/hazelcast/hazelcast-jet',
    gaTrackingId: isLocalhost ? '' : 'UA-158279495-1'
};

module.exports = siteConfig;
