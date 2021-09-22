# Do not edit. bazel-deps autogenerates this file from dependencies/maven/dependencies.yaml.
def _jar_artifact_impl(ctx):
    jar_name = "%s.jar" % ctx.name
    ctx.download(
        output=ctx.path("jar/%s" % jar_name),
        url=ctx.attr.urls,
        sha256=ctx.attr.sha256,
        executable=False
    )
    src_name="%s-sources.jar" % ctx.name
    srcjar_attr=""
    has_sources = len(ctx.attr.src_urls) != 0
    if has_sources:
        ctx.download(
            output=ctx.path("jar/%s" % src_name),
            url=ctx.attr.src_urls,
            sha256=ctx.attr.src_sha256,
            executable=False
        )
        srcjar_attr ='\n    srcjar = ":%s",' % src_name

    build_file_contents = """
package(default_visibility = ['//visibility:public'])
java_import(
    name = 'jar',
    tags = ['maven_coordinates={artifact}'],
    jars = ['{jar_name}'],{srcjar_attr}
)
filegroup(
    name = 'file',
    srcs = [
        '{jar_name}',
        '{src_name}'
    ],
    visibility = ['//visibility:public']
)\n""".format(artifact = ctx.attr.artifact, jar_name = jar_name, src_name = src_name, srcjar_attr = srcjar_attr)
    ctx.file(ctx.path("jar/BUILD"), build_file_contents, False)
    return None

jar_artifact = repository_rule(
    attrs = {
        "artifact": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "urls": attr.string_list(mandatory = True),
        "src_sha256": attr.string(mandatory = False, default=""),
        "src_urls": attr.string_list(mandatory = False, default=[]),
    },
    implementation = _jar_artifact_impl
)

def jar_artifact_callback(hash):
    src_urls = []
    src_sha256 = ""
    source=hash.get("source", None)
    if source != None:
        src_urls = [source["url"]]
        src_sha256 = source["sha256"]
    jar_artifact(
        artifact = hash["artifact"],
        name = hash["name"],
        urls = [hash["url"]],
        sha256 = hash["sha256"],
        src_urls = src_urls,
        src_sha256 = src_sha256
    )
    native.bind(name = hash["bind"], actual = hash["actual"])


def list_dependencies():
    return [
    {"artifact": "ch.qos.logback:logback-classic:1.2.3", "lang": "java", "sha1": "7c4f3c474fb2c041d8028740440937705ebb473a", "sha256": "fb53f8539e7fcb8f093a56e138112056ec1dc809ebb020b59d8a36a5ebac37e0", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/ch/qos/logback/logback-classic/1.2.3/logback-classic-1.2.3.jar", "source": {"sha1": "cfd5385e0c5ed1c8a5dce57d86e79cf357153a64", "sha256": "480cb5e99519271c9256716d4be1a27054047435ff72078d9deae5c6a19f63eb", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/ch/qos/logback/logback-classic/1.2.3/logback-classic-1.2.3-sources.jar"} , "name": "ch-qos-logback-logback-classic", "actual": "@ch-qos-logback-logback-classic//jar", "bind": "jar/ch/qos/logback/logback-classic"},
    {"artifact": "ch.qos.logback:logback-core:1.2.3", "lang": "java", "sha1": "864344400c3d4d92dfeb0a305dc87d953677c03c", "sha256": "5946d837fe6f960c02a53eda7a6926ecc3c758bbdd69aa453ee429f858217f22", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/ch/qos/logback/logback-core/1.2.3/logback-core-1.2.3.jar", "source": {"sha1": "3ebabe69eba0196af9ad3a814f723fb720b9101e", "sha256": "1f69b6b638ec551d26b10feeade5a2b77abe347f9759da95022f0da9a63a9971", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/ch/qos/logback/logback-core/1.2.3/logback-core-1.2.3-sources.jar"} , "name": "ch-qos-logback-logback-core", "actual": "@ch-qos-logback-logback-core//jar", "bind": "jar/ch/qos/logback/logback-core"},
    {"artifact": "com.google.code.findbugs:jsr305:1.3.9", "lang": "java", "sha1": "40719ea6961c0cb6afaeb6a921eaa1f6afd4cfdf", "sha256": "905721a0eea90a81534abb7ee6ef4ea2e5e645fa1def0a5cd88402df1b46c9ed", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar", "name": "com-google-code-findbugs-jsr305", "actual": "@com-google-code-findbugs-jsr305//jar", "bind": "jar/com/google/code/findbugs/jsr305"},
    {"artifact": "com.google.errorprone:error_prone_annotations:2.0.18", "lang": "java", "sha1": "5f65affce1684999e2f4024983835efc3504012e", "sha256": "cb4cfad870bf563a07199f3ebea5763f0dec440fcda0b318640b1feaa788656b", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/errorprone/error_prone_annotations/2.0.18/error_prone_annotations-2.0.18.jar", "source": {"sha1": "220c1232fa6d13b427c10ccc1243a87f5f501d31", "sha256": "dbe7b49dd0584704d5c306b4ac7273556353ea3c0c6c3e50adeeca8df47047be", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/errorprone/error_prone_annotations/2.0.18/error_prone_annotations-2.0.18-sources.jar"} , "name": "com-google-errorprone-error_prone_annotations", "actual": "@com-google-errorprone-error_prone_annotations//jar", "bind": "jar/com/google/errorprone/error-prone-annotations"},
    {"artifact": "com.google.guava:guava:23.0", "lang": "java", "sha1": "c947004bb13d18182be60077ade044099e4f26f1", "sha256": "7baa80df284117e5b945b19b98d367a85ea7b7801bd358ff657946c3bd1b6596", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar", "source": {"sha1": "ed233607c5c11e1a13a3fd760033ed5d9fe525c2", "sha256": "37fe8ba804fb3898c3c8f0cbac319cc9daa58400e5f0226a380ac94fb2c3ca14", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/guava/guava/23.0/guava-23.0-sources.jar"} , "name": "com-google-guava-guava", "actual": "@com-google-guava-guava//jar", "bind": "jar/com/google/guava/guava"},
    {"artifact": "com.google.j2objc:j2objc-annotations:1.1", "lang": "java", "sha1": "ed28ded51a8b1c6b112568def5f4b455e6809019", "sha256": "2994a7eb78f2710bd3d3bfb639b2c94e219cedac0d4d084d516e78c16dddecf6", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/j2objc/j2objc-annotations/1.1/j2objc-annotations-1.1.jar", "source": {"sha1": "1efdf5b737b02f9b72ebdec4f72c37ec411302ff", "sha256": "2cd9022a77151d0b574887635cdfcdf3b78155b602abc89d7f8e62aba55cfb4f", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/j2objc/j2objc-annotations/1.1/j2objc-annotations-1.1-sources.jar"} , "name": "com-google-j2objc-j2objc-annotations", "actual": "@com-google-j2objc-j2objc-annotations//jar", "bind": "jar/com/google/j2objc/j2objc-annotations"},
    {"artifact": "junit:junit:4.12", "lang": "java", "sha1": "2973d150c0dc1fefe998f834810d68f278ea58ec", "sha256": "59721f0805e223d84b90677887d9ff567dc534d7c502ca903c0c2b17f05c116a", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/junit/junit/4.12/junit-4.12.jar", "source": {"sha1": "a6c32b40bf3d76eca54e3c601e5d1470c86fcdfa", "sha256": "9f43fea92033ad82bcad2ae44cec5c82abc9d6ee4b095cab921d11ead98bf2ff", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/junit/junit/4.12/junit-4.12-sources.jar"} , "name": "junit-junit", "actual": "@junit-junit//jar", "bind": "jar/junit/junit"},
    {"artifact": "net.sf.jopt-simple:jopt-simple:4.6", "lang": "java", "sha1": "306816fb57cf94f108a43c95731b08934dcae15c", "sha256": "3fcfbe3203c2ea521bf7640484fd35d6303186ea2e08e72f032d640ca067ffda", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/net/sf/jopt-simple/jopt-simple/4.6/jopt-simple-4.6.jar", "source": {"sha1": "9cd14a61d7aa7d554f251ef285a6f2c65caf7b65", "sha256": "edceaf232b2480e282af8dd9509176507e1781bef92cd06800c2cefed917c85b", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/net/sf/jopt-simple/jopt-simple/4.6/jopt-simple-4.6-sources.jar"} , "name": "net-sf-jopt-simple-jopt-simple", "actual": "@net-sf-jopt-simple-jopt-simple//jar", "bind": "jar/net/sf/jopt-simple/jopt-simple"},
    {"artifact": "org.apache.commons:commons-math3:3.2", "lang": "java", "sha1": "ec2544ab27e110d2d431bdad7d538ed509b21e62", "sha256": "6268a9a0ea3e769fc493a21446664c0ef668e48c93d126791f6f3f757978fee2", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/apache/commons/commons-math3/3.2/commons-math3-3.2.jar", "source": {"sha1": "cd098e055bf192a60c81d81893893e6e31a6482f", "sha256": "b62d60712ea06fb6259506269b3a0ed73a7da5ee11f891c0eb0399eb9bc71e3f", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/apache/commons/commons-math3/3.2/commons-math3-3.2-sources.jar"} , "name": "org-apache-commons-commons-math3", "actual": "@org-apache-commons-commons-math3//jar", "bind": "jar/org/apache/commons/commons-math3"},
    {"artifact": "org.codehaus.mojo:animal-sniffer-annotations:1.14", "lang": "java", "sha1": "775b7e22fb10026eed3f86e8dc556dfafe35f2d5", "sha256": "2068320bd6bad744c3673ab048f67e30bef8f518996fa380033556600669905d", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/codehaus/mojo/animal-sniffer-annotations/1.14/animal-sniffer-annotations-1.14.jar", "source": {"sha1": "886474da3f761d39fcbb723d97ecc5089e731f42", "sha256": "d821ae1f706db2c1b9c88d4b7b0746b01039dac63762745ef3fe5579967dd16b", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/codehaus/mojo/animal-sniffer-annotations/1.14/animal-sniffer-annotations-1.14-sources.jar"} , "name": "org-codehaus-mojo-animal-sniffer-annotations", "actual": "@org-codehaus-mojo-animal-sniffer-annotations//jar", "bind": "jar/org/codehaus/mojo/animal-sniffer-annotations"},
    {"artifact": "org.hamcrest:hamcrest-all:1.3", "lang": "java", "sha1": "63a21ebc981131004ad02e0434e799fd7f3a8d5a", "sha256": "4877670629ab96f34f5f90ab283125fcd9acb7e683e66319a68be6eb2cca60de", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/hamcrest/hamcrest-all/1.3/hamcrest-all-1.3.jar", "source": {"sha1": "47e033b7ab18c5dbd5fe29fc1bd5a40afe028818", "sha256": "c53535c3d25b5bf0b00a324a5583c7dd2fed0fa6d1bbc622e2dec460c24faab3", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/hamcrest/hamcrest-all/1.3/hamcrest-all-1.3-sources.jar"} , "name": "org-hamcrest-hamcrest-all", "actual": "@org-hamcrest-hamcrest-all//jar", "bind": "jar/org/hamcrest/hamcrest-all"},
    {"artifact": "org.hamcrest:hamcrest-core:1.3", "lang": "java", "sha1": "42a25dc3219429f0e5d060061f71acb49bf010a0", "sha256": "66fdef91e9739348df7a096aa384a5685f4e875584cce89386a7a47251c4d8e9", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar", "source": {"sha1": "1dc37250fbc78e23a65a67fbbaf71d2e9cbc3c0b", "sha256": "e223d2d8fbafd66057a8848cc94222d63c3cedd652cc48eddc0ab5c39c0f84df", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3-sources.jar"} , "name": "org-hamcrest-hamcrest-core", "actual": "@org-hamcrest-hamcrest-core//jar", "bind": "jar/org/hamcrest/hamcrest-core"},
    {"artifact": "org.openjdk.jmh:jmh-core:1.23", "lang": "java", "sha1": "eb242d3261f3795c8bf09818d17c3241191284a0", "sha256": "5b202159b21555045affccdde23c57005b9efceaea32ca6e4406d4fe5811e743", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/openjdk/jmh/jmh-core/1.23/jmh-core-1.23.jar", "source": {"sha1": "ade34879d1ee27b89ab4aa856d4fff9b88ee4213", "sha256": "386fb2988fdae424dfec4388a9c0c5a31d77662be4f3576976896d859c286fb6", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/openjdk/jmh/jmh-core/1.23/jmh-core-1.23-sources.jar"} , "name": "org-openjdk-jmh-jmh-core", "actual": "@org-openjdk-jmh-jmh-core//jar", "bind": "jar/org/openjdk/jmh/jmh-core"},
    {"artifact": "org.openjdk.jmh:jmh-generator-annprocess:1.23", "lang": "java", "sha1": "4ea76227ce15d5389a25c005b9b23f7390928fd3", "sha256": "218c80cd06b61097ccd59011480361d4dcbeabf0b280209e781365733d9e7121", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/openjdk/jmh/jmh-generator-annprocess/1.23/jmh-generator-annprocess-1.23.jar", "source": {"sha1": "68d956498bb35c6effc275928cf4815b32e547b9", "sha256": "352acf4b84a6371242f62ae6e908dfdcca630291ade6cf2b0b35da1e7ceedcc6", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/openjdk/jmh/jmh-generator-annprocess/1.23/jmh-generator-annprocess-1.23-sources.jar"} , "name": "org-openjdk-jmh-jmh-generator-annprocess", "actual": "@org-openjdk-jmh-jmh-generator-annprocess//jar", "bind": "jar/org/openjdk/jmh/jmh-generator-annprocess"},
    {"artifact": "org.rocksdb:rocksdbjni:6.6.4", "lang": "java", "sha1": "c0a678885999bdca1bca8ee1226ecc2b9aec7596", "sha256": "a71f1afaa127f7b940cfa81877d6c30a524d3068f469918320e035946f54f71a", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/rocksdb/rocksdbjni/6.6.4/rocksdbjni-6.6.4.jar", "source": {"sha1": "fd184e9a85a7794880759faf82ecee43fd74a12d", "sha256": "3b64d27d6925777428bcf6562f9e436d925b78e90607677e697971c156ad1122", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/rocksdb/rocksdbjni/6.6.4/rocksdbjni-6.6.4-sources.jar"} , "name": "org-rocksdb-rocksdbjni", "actual": "@org-rocksdb-rocksdbjni//jar", "bind": "jar/org/rocksdb/rocksdbjni"},
    {"artifact": "org.slf4j:slf4j-api:1.7.28", "lang": "java", "sha1": "2cd9b264f76e3d087ee21bfc99305928e1bdb443", "sha256": "fb6e4f67a2a4689e3e713584db17a5d1090c1ebe6eec30e9e0349a6ee118141e", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/1.7.28/slf4j-api-1.7.28.jar", "source": {"sha1": "6444f3c8fce32e20f621e264807256c5e65f11c9", "sha256": "b1b8bfa4f2709684606001685d09ef905adc1b72ec53444ade90f44bfbcebcff", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/1.7.28/slf4j-api-1.7.28-sources.jar"} , "name": "org-slf4j-slf4j-api", "actual": "@org-slf4j-slf4j-api//jar", "bind": "jar/org/slf4j/slf4j-api"},
    ]

def maven_dependencies(callback = jar_artifact_callback):
    for hash in list_dependencies():
        callback(hash)
