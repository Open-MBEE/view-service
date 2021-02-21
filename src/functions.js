// TODO Revert after upstream fix for
//  https://github.com/Open-MBEE/confluence-mdk/blob/72d2ff5e67c151bf168c28381d0eb85f8c9ec6de/src/util/neptune.mjs#L15
const {neptuneClear, neptuneLoad, wikiExport, wikiChildPages} = require(/*'confluence-mdk'*/ './confluence-mdk/main/api.mjs');
const {CopyObjectCommand, PutObjectCommand, S3Client} = require('@aws-sdk/client-s3');
const crypto = require('crypto');
const fs = require('fs');
const tmp = require('tmp');
const uuid = require('uuid');

// TODO Separate files for each function to enable better minification and reduce deploy churn on modification
exports.exportConfluencePage = async function (event) {
    const requestId = event.requestId;
    if (!requestId) {
        throw new Error('Request ID not provided');
    }

    const bucket = process.env.S3_BUCKET;
    if (!bucket) {
        throw new Error('Environment variable `S3_BUCKET` not provided');
    }

    const fileObj = tmp.fileSync({
        postfix: '.ttl',
        discardDescriptor: true
    });

    const writeStream = fs.createWriteStream(fileObj.name);
    const exportConfig = _buildExportConfig({
        ...event?.page,
        output: writeStream
    });

    await wikiExport(exportConfig);
    writeStream.close();

    const hashStream = fs.createReadStream(fileObj.name);
    const hash = await _hashFile('sha1', hashStream);
    hashStream.close();

    const s3 = new S3Client({});
    const uploadStream = fs.createReadStream(fileObj.name);
    const key = `${requestId}/${hash}.ttl`;

    await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: uploadStream
    }));
    uploadStream.close();

    fileObj.removeCallback();

    return {
        path: `s3://${bucket}/${key}`,
        sha1: hash
    };
}

function _hashFile(algorithm, stream) {
    return new Promise((resolve, reject) => stream
        .on('error', reject)
        .pipe(crypto.createHash(algorithm)
            .setEncoding('hex'))
        .once('finish', function () {
            resolve(this.read())
        })
    );
}

exports.mapConfluencePageToChildren = async function (event) {
    return _wikiChildPages(_buildExportConfig(event)).then(pages => pages.map(page => {
        return {
            page: page,
            ...event.server && {server: event.server},
            ...event.recurse && {recurse: event.recurse},
            ...event.as_urls && {as_urls: event.as_urls}
        };
    }));
}

exports.popConfluencePageTree = async function (event) {
    const stack = event.stack ?? [];
    const limit = event.limit ?? 1;

    const result = [];
    for (let i = 0; i < limit; i++) {
        const page = stack.pop();
        if (!page) {
            break;
        }
        result.push({
            page: page,
            ...event.server && {server: event.server},
            ...event.as_urls && {as_urls: event.as_urls}
        });
        const children = await _wikiChildPages(_buildExportConfig({
            ...event,
            page: page,
            recurse: false
        }));
        // push to stack in reverse order for left-to-right traversal
        children.reduceRight((_, page) => stack.push(page), null);
    }
    return {
        stack: stack,
        limit: limit,
        result: result,
        ...event.server && {server: event.server},
        ...event.as_urls && {as_urls: event.as_urls}
    }
}

exports.generateUuid = async function (_event) {
    return uuid.v4();
}

exports.flattenArray = async function (event) {
    const isEventArray = Array.isArray(event);
    if (!isEventArray && !Array.isArray(event?.args)) {
        throw new Error('Event must be an array or an object with an `args` key that is an array');
    }
    return (isEventArray ? event : event.args).flat();
}

exports.copyS3Object = async function (event) {
    const bucket = process.env.S3_BUCKET;
    if (!bucket) {
        throw new Error('Environment variable `S3_BUCKET` not provided');
    }
    const source = event.source;
    if (!source) {
        throw new Error('Source not provided');
    }
    const target = event.target;
    if (!target) {
        throw new Error('Target not provided');
    }

    const s3 = new S3Client({});

    return s3.send(new CopyObjectCommand({
        Bucket: bucket,
        CopySource: `${bucket}/${source}`,
        Key: target
    }));
}

exports.clearNeptuneGraph = async function (event) {
    return neptuneClear(_buildImportConfig(event));
}

exports.loadNeptuneGraph = async function (event) {
    return neptuneLoad(_buildImportConfig(event));
}

function _buildExportConfig(options) {
    return {
        page: options.page,
        server: options.server,
        token: process.env.CONFLUENCE_TOKEN,
        ...options.recurse && {recurse: options.recurse},
        ...options.as_urls && {as_urls: options.as_urls},
        ...options.output && {output: options.output}
    }
}

function _buildImportConfig(options) {
    return {
        ...options.graph && {graph: options.graph},
        ...options.prefix && {prefix: options.prefix}
    }
}

// TODO Revert after upstream fix
async function _wikiChildPages(options) {
    return wikiChildPages(options).then(children => children.map(child => !options?.as_urls ? parseInt(child) : child));
}