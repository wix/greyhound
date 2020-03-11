## Publishing maven artifacts

### Setup Bintray credentials

Create the file `~/.bintray/.credentials` with the following content:

```
realm = Bintray API Realm
host = api.bintray.com
user = <user>
password = <your token>
```

### Tag your release

Create a git tag with the format `v<numeric version>`, for example: `v0.1.1`.
Run the following command:

```
sbt publish
```

Update the [adding-to-your-build.md](adding-to-your-build.md) with the new version
