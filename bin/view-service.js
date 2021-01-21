#!/usr/bin/env node

const cdk = require('@aws-cdk/core');
const { ViewServiceStack } = require('../lib/view-service-stack');

const app = new cdk.App();
new ViewServiceStack(app, 'ViewServiceStack');
