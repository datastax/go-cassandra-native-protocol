### Generating mocks

Mock objects are generated with [Mockery](https://github.com/vektra/mockery).

To install Mockery on macOS:

    brew install mockery

For more installation options, see [installation](https://github.com/vektra/mockery#installation). Note that using `go
install` is not officially supported by Mockery.

To re-generate the mocks, run the following commands:

    mockery --dir=./datacodec --name=Codec --output=./datacodec --outpkg=datacodec --filename=mock_codec_test.go --structname=mockCodec
    mockery --dir=./datacodec --name=extractor --output=./datacodec --outpkg=datacodec --filename=mock_extractor_test.go --structname=mockExtractor
    mockery --dir=./datacodec --name=injector --output=./datacodec --outpkg=datacodec --filename=mock_injector_test.go --structname=mockInjector
    mockery --dir=./datacodec --name=keyValueExtractor --output=./datacodec --outpkg=datacodec --filename=mock_key_value_extractor_test.go --structname=mockKeyValueExtractor
    mockery --dir=./datacodec --name=keyValueInjector --output=./datacodec --outpkg=datacodec --filename=mock_key_value_injector_test.go --structname=mockKeyValueInjector

The Makefile has a `mocks` target that will regenerate all mock objects.
