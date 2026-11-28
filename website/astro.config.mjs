import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://anyserve.github.io',
  base: '/anyserve',
  trailingSlash: 'ignore',
  integrations: [
    starlight({
      title: 'AnyServe',
      description: 'Call the GPU under your desk from anywhere. Outbound-only execution and scheduling control plane.',
      favicon: '/favicon.svg',
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/anyserve/anyserve' },
        { icon: 'external', label: 'Live Demo', href: 'https://anyserve.arcosx.org' },
      ],
      editLink: {
        baseUrl: 'https://github.com/anyserve/anyserve/edit/main/website/',
      },
      customCss: ['./src/styles/custom.css'],
      expressiveCode: {
        themes: ['github-light', 'github-dark'],
      },
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { slug: 'getting-started/install' },
            { slug: 'getting-started/quickstart' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { slug: 'guides/python-sdk' },
            { slug: 'guides/configuration' },
            { slug: 'guides/deployment' },
            { slug: 'guides/console' },
          ],
        },
        {
          label: 'Examples',
          items: [
            { slug: 'examples/overview' },
            { slug: 'examples/openai' },
            { slug: 'examples/triton' },
            { slug: 'examples/yolo' },
            { slug: 'examples/vla' },
            { slug: 'examples/comfyui' },
            { slug: 'examples/ros2' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { slug: 'reference/cli' },
            { slug: 'reference/limitations' },
          ],
        },
      ],
    }),
  ],
});
