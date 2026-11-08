# The OCI keys of every image this repository publishes. CI fills the
# variables from the commit it builds, and `docker buildx bake --push`
# writes each key as a config label and as an annotation on the image
# index and each image manifest. The keys live here and not in the
# Dockerfile, so a change to a key invalidates no layer cache, and a
# fork that builds the Dockerfile on its own does not claim to be this
# repository's image.

# SOURCE is the repository's URL, REVISION the commit, VERSION the
# release or development version, and CREATED the commit's committer
# time in RFC 3339. A local build leaves them empty, and an empty key
# is left out.
variable "SOURCE" { default = "" }
variable "REVISION" { default = "" }
variable "VERSION" { default = "" }
variable "CREATED" { default = "" }

# LATEST is "true" on a release, which moves the image's :latest tag.
# A development build pushes its version tag only.
variable "LATEST" { default = "false" }

# The keys that are the same for every image of the repository.
variable "URL" { default = "https://github.com/chrisguidry/redis-operator" }
variable "LICENSES" { default = "MIT" }
variable "VENDOR" { default = "Chris Guidry" }

# oci returns the keys of one image: the commit's keys, the
# repository's keys, and the image's own keys, with the image's keys
# last so that they win. A commit key that is empty is left out, so a
# local build writes no source, revision, version, or created. Any
# other key that is "" is written as "", which replaces a label of the
# same key that the base image set.
function "oci" {
  params = [image]
  result = { for key, value in merge(
    { for key, value in {
      source   = SOURCE
      revision = REVISION
      version  = VERSION
      created  = CREATED
    } : key => value if value != "" },
    {
      url      = URL
      licenses = LICENSES
      vendor   = VENDOR
    },
    image,
  ) : "org.opencontainers.image.${key}" => value }
}

# oci_labels and oci_annotations write the keys of oci at the two
# places that readers look: the config, which docker inspect, syft,
# and Trivy read, and the index and manifests, which ghcr.io reads for
# a multi-platform image. A label passes into every image built FROM
# this one and an annotation does not, so a downstream image keeps
# none of these annotations.
function "oci_labels" {
  params = [image]
  result = oci(image)
}

# A local build with --load exports no image index, and BuildKit refuses
# an index annotation there, so only a CI build, which sets REVISION,
# writes annotations.
function "oci_annotations" {
  params = [image]
  result = REVISION == "" ? [] : [for key, value in oci(image) : "index,manifest:${key}=${value}"]
}

variable "redis-operator" {
  default = {
    title         = "redis-operator"
    description   = "A Kubernetes operator that runs Redis and Valkey with failover, backups, and staged upgrades."
    documentation = "https://github.com/chrisguidry/redis-operator#readme"
  }
}

# Attestations and the registry cache need a registry to write to, so
# only a CI build, which sets REVISION, writes them. A local build with
# --load can write neither. The :buildcache tag lets a release of an
# already-built commit rebuild nothing.
target "redis-operator" {
  context    = "."
  dockerfile = "Dockerfile"
  args       = VERSION == "" ? {} : { VERSION = VERSION }
  platforms  = ["linux/amd64"]
  tags = concat(
    ["ghcr.io/chrisguidry/redis-operator:${VERSION == "" ? "dev" : VERSION}"],
    LATEST == "true" ? ["ghcr.io/chrisguidry/redis-operator:latest"] : [],
  )
  labels      = oci_labels(redis-operator)
  annotations = oci_annotations(redis-operator)
  cache-from  = ["type=registry,ref=ghcr.io/chrisguidry/redis-operator:buildcache"]
  cache-to    = REVISION == "" ? [] : ["type=registry,ref=ghcr.io/chrisguidry/redis-operator:buildcache,mode=max"]
  attest      = REVISION == "" ? [] : ["type=provenance,mode=max", "type=sbom"]
}

group "default" {
  targets = ["redis-operator"]
}
