/**
 * Plugin registration. Importing this module (for side effects)
 * populates the panel registry the /analytics dashboard renders.
 *
 * To add a custom plugin: create a directory here exporting a
 * KapturePlugin as its default export, then register it below. See
 * docs/ui-plugins.md for the full walkthrough.
 */

import { registerPlugin } from "@/lib/plugins/registry";

import cardinality from "./cardinality";
import heavyHitters from "./heavy-hitters";
import quantiles from "./quantiles";
import trafficOverview from "./traffic-overview";

registerPlugin(cardinality);
registerPlugin(trafficOverview);
registerPlugin(heavyHitters);
registerPlugin(quantiles);

// registerPlugin(myCustomPlugin);  ← add yours here
