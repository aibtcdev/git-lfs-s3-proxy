import { AwsClient } from "aws4fetch";

const HOMEPAGE = "https://github.com/aibtcdev/git-lfs-s3-proxy";
const EXPIRY = 3600;
const MIME = "application/vnd.git-lfs+json";
const PART_SIZE = 5 * 1024 * 1024; // 5MB minimum part size for S3

const METHOD_FOR = {
  upload: "PUT",
  download: "GET",
};

async function sign(s3, bucket, path, method) {
  const info = { method };
  const signed = await s3.sign(
    new Request(`https://${bucket}/${path}?X-Amz-Expires=${EXPIRY}`, info),
    { aws: { signQuery: true } }
  );
  return signed.url;
}

function parseAuthorization(req) {
  const auth = req.headers.get("Authorization");
  if (!auth) {
    throw new Response(null, { status: 401 });
  }

  const [scheme, encoded] = auth.split(" ");
  if (scheme !== "Basic" || !encoded) {
    throw new Response(null, { status: 400 });
  }

  const buffer = Uint8Array.from(atob(encoded), (c) => c.charCodeAt(0));
  const decoded = new TextDecoder().decode(buffer).normalize();
  const index = decoded.indexOf(":");
  if (index === -1 || /[\0-\x1F\x7F]/.test(decoded)) {
    throw new Response(null, { status: 400 });
  }

  return { user: decoded.slice(0, index), pass: decoded.slice(index + 1) };
}

async function initiateMultipartUpload(s3, bucket, key) {
  const url = await sign(s3, bucket, key, "POST", "uploads=");
  const response = await fetch(url);
  const xml = await response.text();
  const uploadId = xml.match(/<UploadId>(.*?)<\/UploadId>/)[1];
  return uploadId;
}

async function getSignedUrlForPart(s3, bucket, key, uploadId, partNumber) {
  return sign(
    s3,
    bucket,
    key,
    "PUT",
    `partNumber=${partNumber}&uploadId=${uploadId}&`
  );
}

async function completeMultipartUpload(s3, bucket, key, uploadId, parts) {
  const url = await sign(s3, bucket, key, "POST", `uploadId=${uploadId}&`);
  const xml = `
    <CompleteMultipartUpload>
      ${parts
        .map(
          ({ PartNumber, ETag }) => `
        <Part>
          <PartNumber>${PartNumber}</PartNumber>
          <ETag>${ETag}</ETag>
        </Part>
      `
        )
        .join("")}
    </CompleteMultipartUpload>
  `;
  const response = await fetch(url, {
    method: "POST",
    body: xml,
  });
  return response.ok;
}

async function fetch(req, env) {
  const url = new URL(req.url);

  if (url.pathname == "/") {
    if (req.method === "GET") {
      return Response.redirect(HOMEPAGE, 302);
    } else {
      return new Response(null, { status: 405, headers: { Allow: "GET" } });
    }
  }

  if (!url.pathname.endsWith("/objects/batch")) {
    return new Response(null, { status: 404 });
  }

  if (req.method !== "POST") {
    return new Response(null, { status: 405, headers: { Allow: "POST" } });
  }

  // in practice, we'd rather not break out-of-spec clients not setting these
  /*if (!req.headers.get("Accept").startsWith(MIME)
    || !req.headers.get("Content-Type").startsWith(MIME)) {
    return new Response(null, { status: 406 });
  }*/

  const { user, pass } = parseAuthorization(req);
  let s3Options = { accessKeyId: user, secretAccessKey: pass };

  const segments = url.pathname.split("/").slice(1, -2);
  let params = {};
  let bucketIdx = 0;
  for (const segment of segments) {
    const sliceIdx = segment.indexOf("=");
    if (sliceIdx === -1) {
      break;
    } else {
      const key = decodeURIComponent(segment.slice(0, sliceIdx));
      const val = decodeURIComponent(segment.slice(sliceIdx + 1));
      s3Options[key] = val;

      bucketIdx++;
    }
  }

  const s3 = new AwsClient(s3Options);
  const bucket = segments.slice(bucketIdx).join("/");
  const expires_in = params.expiry || env.EXPIRY || EXPIRY;

  const { objects, operation } = await req.json();
  const method = METHOD_FOR[operation];
  const response = JSON.stringify({
    transfer: "basic",
    objects: await Promise.all(
      objects.map(async ({ oid, size }) => {
        if (operation === "upload" && size > PART_SIZE) {
          // initiate multipart upload
          const uploadId = await initiateMultipartUpload(s3, bucket, oid);
          const partCount = Math.ceil(size / PART_SIZE);
          const parts = await Promise.all(
            Array.from({ length: partCount }, (_, i) =>
              getSignedUrlForPart(s3, bucket, oid, uploadId, i + 1)
            )
          );

          return {
            oid,
            size,
            authenticated: true,
            actions: {
              upload: {
                href: parts[0], // URL for the first part
                header: {
                  "Upload-ID": uploadId,
                  "Part-Count": partCount.toString(),
                },
              },
            },
          };
        } else {
          return {
            oid,
            size,
            authenticated: true,
            actions: {
              [operation]: {
                href: await sign(s3, bucket, oid, method),
                expires_in: expires_in,
              },
            },
          };
        }
      })
    ),
  });

  return new Response(response, {
    status: 200,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": MIME,
    },
  });
}

export default { fetch };
