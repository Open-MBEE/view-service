import {neptuneClear, neptuneLoad, wikiExport, wikiChildPages} from 'confluence-mdk';
import {CopyObjectCommand, PutObjectCommand, S3Client} from '@aws-sdk/client-s3';
import crypto from 'crypto';
import fs from 'fs';
import tmp from 'tmp';
import {v4 as uuidv4} from 'uuid';

// TODO Separate files for each function to enable better minification and reduce deploy churn on modification
export async function exportConfluencePage(event) {
    const requestId = event.requestId;
    if(!requestId) {
        throw new Error('Request ID not provided');
    }

    const bucket = process.env.S3_BUCKET;
    if(!bucket) {
        throw new Error('Environment variable `S3_BUCKET` not provided');
    }

    const fileObj = tmp.fileSync({
        postfix: '.ttl',
        discardDescriptor: true,
    });

    const writeStream = fs.createWriteStream(fileObj.name);
    const exportConfig = _buildExportConfig({
        ...event?.page,
        output: writeStream,
    });

    await wikiExport(exportConfig);

    const hashStream = fs.createReadStream(fileObj.name);
    const hash = await _hashFile('sha1', hashStream);

    const s3 = new S3Client({});
    const uploadStream = fs.createReadStream(fileObj.name);
    const key = `${requestId}/${hash}.ttl`;

    await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: uploadStream,
    }));

    fileObj.removeCallback();

    return {
        path: `s3://${bucket}/${key}`,
        sha1: hash,
    };
}

function _hashFile(algorithm, stream) {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash(algorithm);
        stream
            .on('error', reject)
            .on('data', chunk => hash.update(chunk))
            .on('end', () => resolve(hash.digest('hex')));
    });
}

export async function mapConfluencePageToChildren(event) {
    const pages = await wikiChildPages(_buildExportConfig(event));
    return pages.map(page => ({
        page: page,
        ...event.server && {server: event.server},
        ...event.recurse && {recurse: event.recurse},
        ...event.as_urls && {as_urls: event.as_urls},
    }));
}

export async function popConfluencePageTree(event) {
    const stack = event.stack ?? [];
    const limit = event.limit ?? 1;

    const result = [];
    for(let i = 0; i < limit; i++) {
        const page = stack.pop();
        if(!page) {
            break;
        }
        result.push({
            page: page,
            ...event.server && {server: event.server},
            ...event.as_urls && {as_urls: event.as_urls},
        });
        const children = await wikiChildPages(_buildExportConfig({
            ...event,
            page: page,
            recurse: false,
        }));
        // push to stack in reverse order for left-to-right traversal
        children.reverse().forEach(child => stack.push(child));
    }
    return {
        stack: stack,
        limit: limit,
        result: result,
        ...event.server && {server: event.server},
        ...event.as_urls && {as_urls: event.as_urls},
    };
}

// eslint-disable-next-line require-await
export async function generateUuid() {
    return Promise.resolve(uuidv4());
}

// eslint-disable-next-line require-await
export async function flattenArray(event) {
    const isEventArray = Array.isArray(event);
    if(!isEventArray && !Array.isArray(event?.args)) {
        throw new Error('Event must be an array or an object with an `args` key that is an array');
    }
    return (isEventArray ? event : event.args).flat();
}

export function copyS3Object(event) {
    const bucket = process.env.S3_BUCKET;
    if(!bucket) {
        throw new Error('Environment variable `S3_BUCKET` not provided');
    }
    const source = event.source;
    if(!source) {
        throw new Error('Source not provided');
    }
    const target = event.target;
    if(!target) {
        throw new Error('Target not provided');
    }

    const s3 = new S3Client({});

    return s3.send(new CopyObjectCommand({
        Bucket: bucket,
        CopySource: `${bucket}/${source}`,
        Key: target,
    }));
}

export function clearNeptuneGraph(event) {
    return neptuneClear(_buildImportConfig(event));
}

export function loadNeptuneGraph(event) {
    return neptuneLoad(_buildImportConfig(event));
}

function _buildExportConfig(options) {
    return {
        page: options.page,
        server: options.server,
        token: process.env.CONFLUENCE_TOKEN,
        ...options.recurse && {recurse: options.recurse},
        ...options.as_urls && {as_urls: options.as_urls},
        ...options.output && {output: options.output},
    };
}

function _buildImportConfig(options) {
    return {
        ...options.graph && {graph: options.graph},
        ...options.prefix && {prefix: options.prefix},
    };
}
