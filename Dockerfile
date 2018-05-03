FROM golang:1.7

RUN go get github.com/jstemmer/go-junit-report

COPY . /go/src/github.com/cyverse-de/road-runner
RUN go install github.com/cyverse-de/road-runner

ENTRYPOINT ["road-runner"]
CMD ["--help"]

ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"
LABEL org.label-schema.vcs-ref="$git_commit"
LABEL org.label-schema.vcs-url="https://github.com/cyverse-de/road-runner"
LABEL org.label-schema.version="$descriptive_version"
