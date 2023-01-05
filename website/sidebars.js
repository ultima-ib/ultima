/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import("@docusaurus/plugin-content-docs").SidebarsConfig} */
const sidebars = {
    tutorialSidebar: [
        "intro",
        {
            type: "category",
            label: "FRTB",
            link: {type: 'doc', id: 'frtb/intro'},
            items: [
                "frtb/installation",
                "frtb/analyse",
                "frtb/calculate",
                "frtb/capital",
                "frtb/format",
                "frtb/input",
                "frtb/performance",
                "frtb/weights",
            ],
        },
    ],
}

module.exports = sidebars
