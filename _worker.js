import { AwsClient } from "aws4fetch";

const HOMEPAGE = "https://github.com/aibtcdev/git-lfs-s3-proxy";
const EXPIRY = 3600;
const PART_SIZE = 5 * 1024 * 1024; // 5MB minimum part size for R2

async function sign(s3, bucket, path, method, query = "") {
  const encodedPath = path.split("/").map(encodeURIComponent).join("/");
  const encodedQuery = query ? `?${query}` : "";
  const url = `https://${bucket}/${encodedPath}${encodedQuery}`;

  const info = {
    method,
    headers: {
      "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    },
  };

  const signed = await s3.sign(new Request(url, info), {
    aws: { signQuery: true },
  });

  // logging all in one place
  console.log("=== sign function ===", {
    s3: JSON.stringify(s3),
    bucket,
    path,
    method,
    query,
    url,
    info: JSON.stringify(info),
    signedUrl: signed.url,
  });

  return signed.url;
}

function parseAuthorization(req) {
  const auth = req.headers.get("Authorization");
  if (!auth) {
    throw new Response("Authorization header not found", { status: 401 });
  }

  const [scheme, encoded] = auth.split(" ");
  if (scheme !== "Basic" || !encoded) {
    throw new Response("Invalid authorization scheme or credentials", {
      status: 400,
    });
  }

  const buffer = Uint8Array.from(atob(encoded), (c) => c.charCodeAt(0));
  const decoded = new TextDecoder().decode(buffer).normalize();
  const index = decoded.indexOf(":");
  if (index === -1 || /[\0-\x1F\x7F]/.test(decoded)) {
    throw new Response("Unable to decode authorization", { status: 400 });
  }

  return { user: decoded.slice(0, index), pass: decoded.slice(index + 1) };
}

async function initiateMultipartUpload(s3, bucket, prefix, oid) {
  const key = `${prefix}/${oid}`;
  console.log("=== initiateMultipartUpload ===", { bucket, key });

  try {
    const signedUrl = await sign(s3, bucket, key, "POST", "uploads=");
    const encodedUrl = encodeURI(signedUrl);

    console.log("Initiating multipart upload request:", {
      method: "POST",
      signedUrl,
      encodedUrl,
      headers: {
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
        "Content-Type": "application/octet-stream",
      },
    });

    const response = await fetch(encodedUrl, {
      method: "POST",
      headers: {
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
        "Content-Type": "application/octet-stream",
      },
    }).catch((error) => {
      console.error("Error in fetch for initiateMultipartUpload:", error);
    });
    const responseBody = await response.text();

    console.log("=== Response Info ===", {
      status: response.status,
      headers: Object.fromEntries(response.headers),
      body: responseBody,
      encodedUrl,
    });

    if (!response.ok) {
      throw new Error(
        `R2 responded with status ${response.status}: ${responseBody}`
      );
    }

    const uploadId = responseBody.match(/<UploadId>(.*?)<\/UploadId>/)[1];
    if (!uploadId) {
      throw new Error("Failed to extract UploadId from R2 response");
    }
    return uploadId;
  } catch (error) {
    console.error(
      `Error in initiateMultipartUpload for bucket ${bucket}, key ${key}:`,
      error
    );
    if (error.response) {
      console.error("Response data:", await error.response.text());
    }
    throw error;
  }
}

async function handleMultipartUpload(s3, bucket, prefix, oid, size) {
  console.log("=== handleMultipartUpload ===", {
    bucket,
    prefix,
    oid,
    size,
  });
  try {
    const uploadId = await initiateMultipartUpload(s3, bucket, prefix, oid);
    const partCount = Math.ceil(size / PART_SIZE);

    const partUrls = await Promise.all(
      Array.from({ length: partCount }, (_, i) =>
        getSignedUrlForPart(s3, bucket, `${prefix}/${oid}`, uploadId, i + 1)
      )
    );

    const completeUrl = await getSignedUrlForCompletion(
      s3,
      bucket,
      `${prefix}/${oid}`,
      uploadId
    );

    console.log("Multipart upload initialized:", {
      uploadId,
      partCount,
      partUrls,
      completeUrl,
    });

    return {
      uploadId,
      partUrls,
      completeUrl,
      partCount,
    };
  } catch (error) {
    console.error(
      `Error in handleMultipartUpload for ${prefix}/${oid}:`,
      error
    );
    if (error.name === "AbortError") {
      throw new Error("Multipart upload initialization timed out");
    } else if (error.message.includes("AccessDenied")) {
      throw new Error("Access denied. Check R2 credentials and permissions.");
    } else if (error.message.includes("NoSuchBucket")) {
      throw new Error("R2 bucket not found. Check bucket name and region.");
    } else if (error.message.includes("NetworkError")) {
      throw new Error(
        "Network error. Check your internet connection and try again."
      );
    } else {
      throw new Error(
        `Failed to initialize multipart upload, unknown error: ${error.message}`
      );
    }
  }
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
    // const bucket = segments.slice(bucketIdx).join("/");
    const bucket = segments[bucketIdx]; // 'bucket.aibtc.dev'
    const prefix = segments.slice(bucketIdx + 1).join("/"); // 'aibtcdev-communications'
    const expires_in = params.expiry || env.EXPIRY || EXPIRY;

    console.log("=== fetch ===", {
      user,
      pass,
      s3Options: JSON.stringify(s3Options),
      bucket,
      prefix,
      expires_in,
    });

    const { objects, operation } = await req.json();

    const processedObjects = await Promise.all(
      objects.map(async ({ oid, size }) => {
        try {
          if (operation === "upload" && size > PART_SIZE) {
            const { uploadId, partUrls, completeUrl, partCount } =
              await handleMultipartUpload(s3, bucket, prefix, oid, size);

            return {
              oid,
              size,
              authenticated: true,
              actions: {
                upload: partUrls.map((url, index) => ({
                  href: url,
                  header: {
                    "Content-Type": "application/octet-stream",
                  },
                  expires_in: expires_in,
                  partNumber: index + 1,
                })),
                verify: {
                  href: completeUrl,
                  header: {
                    "Content-Type": "application/xml",
                  },
                  expires_in: expires_in,
                },
              },
              multipart: {
                partSize: PART_SIZE,
                partCount: partCount,
                uploadId: uploadId,
              },
            };
          } else {
            const href = await sign(
              s3,
              bucket,
              `${prefix}/${oid}`,
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
                      operation === "upload" ? "application/octet-stream" : "",
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
              message:
                error.message || "Internal server error processing object",
            },
          };
        }
      })
    );

    const response = JSON.stringify({
      transfer: "basic",
      objects: processedObjects,
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
    if (error instanceof Response) {
      // handle errors thrown as Response objects (e.g., 401, 400)
      return error;
    } else if (error.name === "AbortError") {
      return new Response("Request timed out", { status: 504 });
    } else if (error.name === "TypeError") {
      return new Response("Bad request format", { status: 400 });
    } else if (error.message.includes("NetworkError")) {
      return new Response("Network error occurred", { status: 503 });
    } else {
      // generic server error for unhandled cases
      return new Response("Internal server error", { status: 500 });
    }
  }
}

export default { fetch };
