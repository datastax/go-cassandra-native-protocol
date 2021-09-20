### Generating mocks

Mocks where generating using Mockery.

    brew install mockery

To re-generate:

    mockery --dir=./datacodec --name=Codec --output=./datacodec --outpkg=datacodec --filename=mock_codec_test.go --structname=mockCodec
    mockery --dir=./datacodec --name=extractor --output=./datacodec --outpkg=datacodec --filename=mock_extractor_test.go --structname=mockExtractor
    mockery --dir=./datacodec --name=injector --output=./datacodec --outpkg=datacodec --filename=mock_injector_test.go --structname=mockInjector
    mockery --dir=./datacodec --name=keyValueExtractor --output=./datacodec --outpkg=datacodec --filename=mock_key_value_extractor_test.go --structname=mockKeyValueExtractor
    mockery --dir=./datacodec --name=keyValueInjector --output=./datacodec --outpkg=datacodec --filename=mock_key_value_injector_test.go --structname=mockKeyValueInjector
