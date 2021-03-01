#!/usr/bin/env node

import cdk from '@aws-cdk/core';
import {ViewServiceStack} from '../lib/view-service-stack.mjs';

const app = new cdk.App();
new ViewServiceStack(app, 'ViewServiceStack', {
    // TODO Replace with explicit input
    //  ref: https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    //  ref: https://stackoverflow.com/a/64692164
    env: {
        account: process.env.CDK_DEFAULT_ACCOUNT,
        region: process.env.CDK_DEFAULT_REGION,
    },
});
