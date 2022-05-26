//go:build !ignore_autogenerated
// +build !ignore_autogenerated

// Copyright 2022 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by deepcopy-gen. DO NOT EDIT.

package message

import (
	primitive "github.com/datastax/go-cassandra-native-protocol/primitive"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *AlreadyExists) DeepCopyInto(out *AlreadyExists) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new AlreadyExists.
func (in *AlreadyExists) DeepCopy() *AlreadyExists {
	if in == nil {
		return nil
	}
	out := new(AlreadyExists)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *AlreadyExists) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *AuthChallenge) DeepCopyInto(out *AuthChallenge) {
	*out = *in
	if in.Token != nil {
		in, out := &in.Token, &out.Token
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new AuthChallenge.
func (in *AuthChallenge) DeepCopy() *AuthChallenge {
	if in == nil {
		return nil
	}
	out := new(AuthChallenge)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *AuthChallenge) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *AuthResponse) DeepCopyInto(out *AuthResponse) {
	*out = *in
	if in.Token != nil {
		in, out := &in.Token, &out.Token
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new AuthResponse.
func (in *AuthResponse) DeepCopy() *AuthResponse {
	if in == nil {
		return nil
	}
	out := new(AuthResponse)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *AuthResponse) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *AuthSuccess) DeepCopyInto(out *AuthSuccess) {
	*out = *in
	if in.Token != nil {
		in, out := &in.Token, &out.Token
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new AuthSuccess.
func (in *AuthSuccess) DeepCopy() *AuthSuccess {
	if in == nil {
		return nil
	}
	out := new(AuthSuccess)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *AuthSuccess) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Authenticate) DeepCopyInto(out *Authenticate) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Authenticate.
func (in *Authenticate) DeepCopy() *Authenticate {
	if in == nil {
		return nil
	}
	out := new(Authenticate)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Authenticate) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *AuthenticationError) DeepCopyInto(out *AuthenticationError) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new AuthenticationError.
func (in *AuthenticationError) DeepCopy() *AuthenticationError {
	if in == nil {
		return nil
	}
	out := new(AuthenticationError)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *AuthenticationError) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Batch) DeepCopyInto(out *Batch) {
	*out = *in
	if in.Children != nil {
		in, out := &in.Children, &out.Children
		*out = make([]*BatchChild, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = new(BatchChild)
				(*in).DeepCopyInto(*out)
			}
		}
	}
	if in.SerialConsistency != nil {
		in, out := &in.SerialConsistency, &out.SerialConsistency
		*out = new(primitive.NillableConsistencyLevel)
		**out = **in
	}
	if in.DefaultTimestamp != nil {
		in, out := &in.DefaultTimestamp, &out.DefaultTimestamp
		*out = new(primitive.NillableInt64)
		**out = **in
	}
	if in.NowInSeconds != nil {
		in, out := &in.NowInSeconds, &out.NowInSeconds
		*out = new(primitive.NillableInt32)
		**out = **in
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Batch.
func (in *Batch) DeepCopy() *Batch {
	if in == nil {
		return nil
	}
	out := new(Batch)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Batch) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *BatchChild) DeepCopyInto(out *BatchChild) {
	*out = *in
	if in.Id != nil {
		in, out := &in.Id, &out.Id
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	if in.Values != nil {
		in, out := &in.Values, &out.Values
		*out = make([]*primitive.Value, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = new(primitive.Value)
				(*in).DeepCopyInto(*out)
			}
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new BatchChild.
func (in *BatchChild) DeepCopy() *BatchChild {
	if in == nil {
		return nil
	}
	out := new(BatchChild)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ColumnMetadata) DeepCopyInto(out *ColumnMetadata) {
	*out = *in
	if in.Type != nil {
		out.Type = in.Type.DeepCopyDataType()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ColumnMetadata.
func (in *ColumnMetadata) DeepCopy() *ColumnMetadata {
	if in == nil {
		return nil
	}
	out := new(ColumnMetadata)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ConfigError) DeepCopyInto(out *ConfigError) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ConfigError.
func (in *ConfigError) DeepCopy() *ConfigError {
	if in == nil {
		return nil
	}
	out := new(ConfigError)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *ConfigError) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ContinuousPagingOptions) DeepCopyInto(out *ContinuousPagingOptions) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ContinuousPagingOptions.
func (in *ContinuousPagingOptions) DeepCopy() *ContinuousPagingOptions {
	if in == nil {
		return nil
	}
	out := new(ContinuousPagingOptions)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Execute) DeepCopyInto(out *Execute) {
	*out = *in
	if in.QueryId != nil {
		in, out := &in.QueryId, &out.QueryId
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	if in.ResultMetadataId != nil {
		in, out := &in.ResultMetadataId, &out.ResultMetadataId
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	if in.Options != nil {
		in, out := &in.Options, &out.Options
		*out = new(QueryOptions)
		(*in).DeepCopyInto(*out)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Execute.
func (in *Execute) DeepCopy() *Execute {
	if in == nil {
		return nil
	}
	out := new(Execute)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Execute) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *FunctionFailure) DeepCopyInto(out *FunctionFailure) {
	*out = *in
	if in.Arguments != nil {
		in, out := &in.Arguments, &out.Arguments
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new FunctionFailure.
func (in *FunctionFailure) DeepCopy() *FunctionFailure {
	if in == nil {
		return nil
	}
	out := new(FunctionFailure)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *FunctionFailure) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Invalid) DeepCopyInto(out *Invalid) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Invalid.
func (in *Invalid) DeepCopy() *Invalid {
	if in == nil {
		return nil
	}
	out := new(Invalid)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Invalid) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *IsBootstrapping) DeepCopyInto(out *IsBootstrapping) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new IsBootstrapping.
func (in *IsBootstrapping) DeepCopy() *IsBootstrapping {
	if in == nil {
		return nil
	}
	out := new(IsBootstrapping)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *IsBootstrapping) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Options) DeepCopyInto(out *Options) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Options.
func (in *Options) DeepCopy() *Options {
	if in == nil {
		return nil
	}
	out := new(Options)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Options) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Overloaded) DeepCopyInto(out *Overloaded) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Overloaded.
func (in *Overloaded) DeepCopy() *Overloaded {
	if in == nil {
		return nil
	}
	out := new(Overloaded)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Overloaded) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Prepare) DeepCopyInto(out *Prepare) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Prepare.
func (in *Prepare) DeepCopy() *Prepare {
	if in == nil {
		return nil
	}
	out := new(Prepare)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Prepare) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *PreparedResult) DeepCopyInto(out *PreparedResult) {
	*out = *in
	if in.PreparedQueryId != nil {
		in, out := &in.PreparedQueryId, &out.PreparedQueryId
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	if in.ResultMetadataId != nil {
		in, out := &in.ResultMetadataId, &out.ResultMetadataId
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	if in.VariablesMetadata != nil {
		in, out := &in.VariablesMetadata, &out.VariablesMetadata
		*out = new(VariablesMetadata)
		(*in).DeepCopyInto(*out)
	}
	if in.ResultMetadata != nil {
		in, out := &in.ResultMetadata, &out.ResultMetadata
		*out = new(RowsMetadata)
		(*in).DeepCopyInto(*out)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new PreparedResult.
func (in *PreparedResult) DeepCopy() *PreparedResult {
	if in == nil {
		return nil
	}
	out := new(PreparedResult)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *PreparedResult) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ProtocolError) DeepCopyInto(out *ProtocolError) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ProtocolError.
func (in *ProtocolError) DeepCopy() *ProtocolError {
	if in == nil {
		return nil
	}
	out := new(ProtocolError)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *ProtocolError) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Query) DeepCopyInto(out *Query) {
	*out = *in
	if in.Options != nil {
		in, out := &in.Options, &out.Options
		*out = new(QueryOptions)
		(*in).DeepCopyInto(*out)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Query.
func (in *Query) DeepCopy() *Query {
	if in == nil {
		return nil
	}
	out := new(Query)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Query) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *QueryOptions) DeepCopyInto(out *QueryOptions) {
	*out = *in
	if in.PositionalValues != nil {
		in, out := &in.PositionalValues, &out.PositionalValues
		*out = make([]*primitive.Value, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = new(primitive.Value)
				(*in).DeepCopyInto(*out)
			}
		}
	}
	if in.NamedValues != nil {
		in, out := &in.NamedValues, &out.NamedValues
		*out = make(map[string]*primitive.Value, len(*in))
		for key, val := range *in {
			var outVal *primitive.Value
			if val == nil {
				(*out)[key] = nil
			} else {
				in, out := &val, &outVal
				*out = new(primitive.Value)
				(*in).DeepCopyInto(*out)
			}
			(*out)[key] = outVal
		}
	}
	if in.PagingState != nil {
		in, out := &in.PagingState, &out.PagingState
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	if in.SerialConsistency != nil {
		in, out := &in.SerialConsistency, &out.SerialConsistency
		*out = new(primitive.NillableConsistencyLevel)
		**out = **in
	}
	if in.DefaultTimestamp != nil {
		in, out := &in.DefaultTimestamp, &out.DefaultTimestamp
		*out = new(primitive.NillableInt64)
		**out = **in
	}
	if in.NowInSeconds != nil {
		in, out := &in.NowInSeconds, &out.NowInSeconds
		*out = new(primitive.NillableInt32)
		**out = **in
	}
	if in.ContinuousPagingOptions != nil {
		in, out := &in.ContinuousPagingOptions, &out.ContinuousPagingOptions
		*out = new(ContinuousPagingOptions)
		**out = **in
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new QueryOptions.
func (in *QueryOptions) DeepCopy() *QueryOptions {
	if in == nil {
		return nil
	}
	out := new(QueryOptions)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReadFailure) DeepCopyInto(out *ReadFailure) {
	*out = *in
	if in.FailureReasons != nil {
		in, out := &in.FailureReasons, &out.FailureReasons
		*out = make([]*primitive.FailureReason, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = new(primitive.FailureReason)
				(*in).DeepCopyInto(*out)
			}
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReadFailure.
func (in *ReadFailure) DeepCopy() *ReadFailure {
	if in == nil {
		return nil
	}
	out := new(ReadFailure)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *ReadFailure) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReadTimeout) DeepCopyInto(out *ReadTimeout) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReadTimeout.
func (in *ReadTimeout) DeepCopy() *ReadTimeout {
	if in == nil {
		return nil
	}
	out := new(ReadTimeout)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *ReadTimeout) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Ready) DeepCopyInto(out *Ready) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Ready.
func (in *Ready) DeepCopy() *Ready {
	if in == nil {
		return nil
	}
	out := new(Ready)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Ready) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Register) DeepCopyInto(out *Register) {
	*out = *in
	if in.EventTypes != nil {
		in, out := &in.EventTypes, &out.EventTypes
		*out = make([]primitive.EventType, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Register.
func (in *Register) DeepCopy() *Register {
	if in == nil {
		return nil
	}
	out := new(Register)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Register) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Revise) DeepCopyInto(out *Revise) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Revise.
func (in *Revise) DeepCopy() *Revise {
	if in == nil {
		return nil
	}
	out := new(Revise)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Revise) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RowsMetadata) DeepCopyInto(out *RowsMetadata) {
	*out = *in
	if in.PagingState != nil {
		in, out := &in.PagingState, &out.PagingState
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	if in.NewResultMetadataId != nil {
		in, out := &in.NewResultMetadataId, &out.NewResultMetadataId
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	if in.Columns != nil {
		in, out := &in.Columns, &out.Columns
		*out = make([]*ColumnMetadata, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = new(ColumnMetadata)
				(*in).DeepCopyInto(*out)
			}
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RowsMetadata.
func (in *RowsMetadata) DeepCopy() *RowsMetadata {
	if in == nil {
		return nil
	}
	out := new(RowsMetadata)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RowsResult) DeepCopyInto(out *RowsResult) {
	*out = *in
	if in.Metadata != nil {
		in, out := &in.Metadata, &out.Metadata
		*out = new(RowsMetadata)
		(*in).DeepCopyInto(*out)
	}
	if in.Data != nil {
		in, out := &in.Data, &out.Data
		*out = make([][][]byte, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = make([][]byte, len(*in))
				for i := range *in {
					if (*in)[i] != nil {
						in, out := &(*in)[i], &(*out)[i]
						*out = make([]byte, len(*in))
						copy(*out, *in)
					}
				}
			}
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RowsResult.
func (in *RowsResult) DeepCopy() *RowsResult {
	if in == nil {
		return nil
	}
	out := new(RowsResult)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *RowsResult) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SchemaChangeEvent) DeepCopyInto(out *SchemaChangeEvent) {
	*out = *in
	if in.Arguments != nil {
		in, out := &in.Arguments, &out.Arguments
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SchemaChangeEvent.
func (in *SchemaChangeEvent) DeepCopy() *SchemaChangeEvent {
	if in == nil {
		return nil
	}
	out := new(SchemaChangeEvent)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *SchemaChangeEvent) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SchemaChangeResult) DeepCopyInto(out *SchemaChangeResult) {
	*out = *in
	if in.Arguments != nil {
		in, out := &in.Arguments, &out.Arguments
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SchemaChangeResult.
func (in *SchemaChangeResult) DeepCopy() *SchemaChangeResult {
	if in == nil {
		return nil
	}
	out := new(SchemaChangeResult)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *SchemaChangeResult) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ServerError) DeepCopyInto(out *ServerError) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ServerError.
func (in *ServerError) DeepCopy() *ServerError {
	if in == nil {
		return nil
	}
	out := new(ServerError)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *ServerError) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SetKeyspaceResult) DeepCopyInto(out *SetKeyspaceResult) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SetKeyspaceResult.
func (in *SetKeyspaceResult) DeepCopy() *SetKeyspaceResult {
	if in == nil {
		return nil
	}
	out := new(SetKeyspaceResult)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *SetKeyspaceResult) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Startup) DeepCopyInto(out *Startup) {
	*out = *in
	if in.Options != nil {
		in, out := &in.Options, &out.Options
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Startup.
func (in *Startup) DeepCopy() *Startup {
	if in == nil {
		return nil
	}
	out := new(Startup)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Startup) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *StatusChangeEvent) DeepCopyInto(out *StatusChangeEvent) {
	*out = *in
	if in.Address != nil {
		in, out := &in.Address, &out.Address
		*out = new(primitive.Inet)
		(*in).DeepCopyInto(*out)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new StatusChangeEvent.
func (in *StatusChangeEvent) DeepCopy() *StatusChangeEvent {
	if in == nil {
		return nil
	}
	out := new(StatusChangeEvent)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *StatusChangeEvent) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Supported) DeepCopyInto(out *Supported) {
	*out = *in
	if in.Options != nil {
		in, out := &in.Options, &out.Options
		*out = make(map[string][]string, len(*in))
		for key, val := range *in {
			var outVal []string
			if val == nil {
				(*out)[key] = nil
			} else {
				in, out := &val, &outVal
				*out = make([]string, len(*in))
				copy(*out, *in)
			}
			(*out)[key] = outVal
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Supported.
func (in *Supported) DeepCopy() *Supported {
	if in == nil {
		return nil
	}
	out := new(Supported)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Supported) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SyntaxError) DeepCopyInto(out *SyntaxError) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SyntaxError.
func (in *SyntaxError) DeepCopy() *SyntaxError {
	if in == nil {
		return nil
	}
	out := new(SyntaxError)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *SyntaxError) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TopologyChangeEvent) DeepCopyInto(out *TopologyChangeEvent) {
	*out = *in
	if in.Address != nil {
		in, out := &in.Address, &out.Address
		*out = new(primitive.Inet)
		(*in).DeepCopyInto(*out)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TopologyChangeEvent.
func (in *TopologyChangeEvent) DeepCopy() *TopologyChangeEvent {
	if in == nil {
		return nil
	}
	out := new(TopologyChangeEvent)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *TopologyChangeEvent) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TruncateError) DeepCopyInto(out *TruncateError) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TruncateError.
func (in *TruncateError) DeepCopy() *TruncateError {
	if in == nil {
		return nil
	}
	out := new(TruncateError)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *TruncateError) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Unauthorized) DeepCopyInto(out *Unauthorized) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Unauthorized.
func (in *Unauthorized) DeepCopy() *Unauthorized {
	if in == nil {
		return nil
	}
	out := new(Unauthorized)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Unauthorized) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Unavailable) DeepCopyInto(out *Unavailable) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Unavailable.
func (in *Unavailable) DeepCopy() *Unavailable {
	if in == nil {
		return nil
	}
	out := new(Unavailable)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Unavailable) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Unprepared) DeepCopyInto(out *Unprepared) {
	*out = *in
	if in.Id != nil {
		in, out := &in.Id, &out.Id
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Unprepared.
func (in *Unprepared) DeepCopy() *Unprepared {
	if in == nil {
		return nil
	}
	out := new(Unprepared)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *Unprepared) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *VariablesMetadata) DeepCopyInto(out *VariablesMetadata) {
	*out = *in
	if in.PkIndices != nil {
		in, out := &in.PkIndices, &out.PkIndices
		*out = make([]uint16, len(*in))
		copy(*out, *in)
	}
	if in.Columns != nil {
		in, out := &in.Columns, &out.Columns
		*out = make([]*ColumnMetadata, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = new(ColumnMetadata)
				(*in).DeepCopyInto(*out)
			}
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new VariablesMetadata.
func (in *VariablesMetadata) DeepCopy() *VariablesMetadata {
	if in == nil {
		return nil
	}
	out := new(VariablesMetadata)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *VoidResult) DeepCopyInto(out *VoidResult) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new VoidResult.
func (in *VoidResult) DeepCopy() *VoidResult {
	if in == nil {
		return nil
	}
	out := new(VoidResult)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *VoidResult) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *WriteFailure) DeepCopyInto(out *WriteFailure) {
	*out = *in
	if in.FailureReasons != nil {
		in, out := &in.FailureReasons, &out.FailureReasons
		*out = make([]*primitive.FailureReason, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = new(primitive.FailureReason)
				(*in).DeepCopyInto(*out)
			}
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new WriteFailure.
func (in *WriteFailure) DeepCopy() *WriteFailure {
	if in == nil {
		return nil
	}
	out := new(WriteFailure)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *WriteFailure) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *WriteTimeout) DeepCopyInto(out *WriteTimeout) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new WriteTimeout.
func (in *WriteTimeout) DeepCopy() *WriteTimeout {
	if in == nil {
		return nil
	}
	out := new(WriteTimeout)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyMessage is an autogenerated deepcopy function, copying the receiver, creating a new Message.
func (in *WriteTimeout) DeepCopyMessage() Message {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
