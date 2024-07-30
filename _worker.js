import { AwsClient } from "aws4fetch";

const HOMEPAGE = "https://github.com/aibtcdev/git-lfs-s3-proxy";
const EXPIRY = 3600;
const MIME = "application/vnd.git-lfs+json";
const PART_SIZE = 5 * 1024 * 1024; // 5MB minimum part size for S3

const METHOD_FOR = {
  upload: "PUT",
  download: "GET",
};

async function sign(s3, bucket, path, method, query = "") {
  const info = { method };
  const signed = await s3.sign(
    new Request(
      `https://${bucket}/${path}?${query}X-Amz-Expires=${EXPIRY}`,
      info
    ),
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

async function getSignedUrlForCompletion(s3, bucket, key, uploadId) {
  return sign(s3, bucket, key, "POST", `uploadId=${uploadId}&`);
}
async function fetch(req, env) {
  try {
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

    const response = JSON.stringify({
      transfer: "basic",
      objects: await Promise.all(
        objects.map(async ({ oid, size }) => {
          try {
            if (operation === "upload" && size > PART_SIZE) {
              // initiate multipart upload
              const uploadId = await initiateMultipartUpload(s3, bucket, oid);
              const partCount = Math.ceil(size / PART_SIZE);

              // generate signed URLs for all parts
              const partUrls = await Promise.all(
                Array.from({ length: partCount }, (_, i) =>
                  getSignedUrlForPart(s3, bucket, oid, uploadId, i + 1)
                )
              );

              // generate signed URL for completing the multipart upload
              const completeUrl = await getSignedUrlForCompletion(
                s3,
                bucket,
                oid,
                uploadId
              );

              return {
                oid,
                size,
                authenticated: true,
                actions: {
                  upload: {
                    href: partUrls[0],
                    header: {
                      "Content-Type": "application/octet-stream",
                    },
                    expires_in: expires_in,
                  },
                  verify: {
                    href: completeUrl,
                    header: {
                      "Content-Type": "application/xml",
                    },
                    expires_in: expires_in,
                  },
                },
                error: {
                  code: 202,
                  message: "Large file detected, using multipart upload",
                },
              };
            } else {
              const href = await sign(
                s3,
                bucket,
                oid,
                operation === "upload" ? "PUT" : "GET"
              );
              return {
                oid,
                size,
                authenticated: true,
                actions: {
                  [operation]: {
                    href,
                    header: {
                      "Content-Type":
                        operation === "upload"
                          ? "application/octet-stream"
                          : "",
                    },
                    expires_in,
                  },
                },
              };
            }
          } catch (error) {
            console.error(`Error processing object ${oid}:`, error);
            return {
              oid,
              size,
              error: {
                message: "Internal server error processing object",
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
        "Content-Type": "application/vnd.git-lfs+json",
      },
    });
  } catch (error) {
    console.error("Unexpected error:", error);
    return new Response(JSON.stringify({ message: "Internal server error" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}

export default { fetch };
