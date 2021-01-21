const { expect, matchTemplate, MatchStyle } = require('@aws-cdk/assert');
const cdk = require('@aws-cdk/core');
const ViewService = require('../lib/view-service-stack');

test('Empty Stack', () => {
    const app = new cdk.App();
    // WHEN
    const stack = new ViewService.ViewServiceStack(app, 'MyTestStack');
    // THEN
    expect(stack).to(matchTemplate({
      "Resources": {}
    }, MatchStyle.EXACT))
});
