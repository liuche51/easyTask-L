package com.github.liuche51.easyTask.dto.proto;// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ResultDto.proto

public final class ResultDto {
  private ResultDto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface ResultOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required string result = 1;
    /**
     * <code>required string result = 1;</code>
     */
    boolean hasResult();
    /**
     * <code>required string result = 1;</code>
     */
    String getResult();
    /**
     * <code>required string result = 1;</code>
     */
    com.google.protobuf.ByteString
        getResultBytes();

    // optional string msg = 2;
    /**
     * <code>optional string msg = 2;</code>
     */
    boolean hasMsg();
    /**
     * <code>optional string msg = 2;</code>
     */
    String getMsg();
    /**
     * <code>optional string msg = 2;</code>
     */
    com.google.protobuf.ByteString
        getMsgBytes();

    // optional string body = 3;
    /**
     * <code>optional string body = 3;</code>
     */
    boolean hasBody();
    /**
     * <code>optional string body = 3;</code>
     */
    String getBody();
    /**
     * <code>optional string body = 3;</code>
     */
    com.google.protobuf.ByteString
        getBodyBytes();
  }
  /**
   * Protobuf type {@code Result}
   */
  public static final class Result extends
      com.google.protobuf.GeneratedMessage
      implements ResultOrBuilder {
    // Use Result.newBuilder() to construct.
    private Result(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Result(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Result defaultInstance;
    public static Result getDefaultInstance() {
      return defaultInstance;
    }

    public Result getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private Result(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              result_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              msg_ = input.readBytes();
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              body_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return ResultDto.internal_static_Result_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return ResultDto.internal_static_Result_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Result.class, Builder.class);
    }

    public static com.google.protobuf.Parser<Result> PARSER =
        new com.google.protobuf.AbstractParser<Result>() {
      public Result parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Result(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<Result> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required string result = 1;
    public static final int RESULT_FIELD_NUMBER = 1;
    private Object result_;
    /**
     * <code>required string result = 1;</code>
     */
    public boolean hasResult() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string result = 1;</code>
     */
    public String getResult() {
      Object ref = result_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          result_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string result = 1;</code>
     */
    public com.google.protobuf.ByteString
        getResultBytes() {
      Object ref = result_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        result_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional string msg = 2;
    public static final int MSG_FIELD_NUMBER = 2;
    private Object msg_;
    /**
     * <code>optional string msg = 2;</code>
     */
    public boolean hasMsg() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string msg = 2;</code>
     */
    public String getMsg() {
      Object ref = msg_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          msg_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string msg = 2;</code>
     */
    public com.google.protobuf.ByteString
        getMsgBytes() {
      Object ref = msg_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        msg_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional string body = 3;
    public static final int BODY_FIELD_NUMBER = 3;
    private Object body_;
    /**
     * <code>optional string body = 3;</code>
     */
    public boolean hasBody() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional string body = 3;</code>
     */
    public String getBody() {
      Object ref = body_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          body_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string body = 3;</code>
     */
    public com.google.protobuf.ByteString
        getBodyBytes() {
      Object ref = body_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        body_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      result_ = "";
      msg_ = "";
      body_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasResult()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getResultBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getMsgBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, getBodyBytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getResultBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getMsgBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, getBodyBytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static Result parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static Result parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static Result parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static Result parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static Result parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static Result parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static Result parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static Result parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static Result parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static Result parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(Result prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code Result}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements ResultOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return ResultDto.internal_static_Result_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return ResultDto.internal_static_Result_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Result.class, Builder.class);
      }

      // Construct using ResultDto.Result.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        result_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        msg_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        body_ = "";
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return ResultDto.internal_static_Result_descriptor;
      }

      public Result getDefaultInstanceForType() {
        return Result.getDefaultInstance();
      }

      public Result build() {
        Result result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public Result buildPartial() {
        Result result = new Result(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.result_ = result_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.msg_ = msg_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.body_ = body_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof Result) {
          return mergeFrom((Result)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Result other) {
        if (other == Result.getDefaultInstance()) return this;
        if (other.hasResult()) {
          bitField0_ |= 0x00000001;
          result_ = other.result_;
          onChanged();
        }
        if (other.hasMsg()) {
          bitField0_ |= 0x00000002;
          msg_ = other.msg_;
          onChanged();
        }
        if (other.hasBody()) {
          bitField0_ |= 0x00000004;
          body_ = other.body_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasResult()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        Result parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (Result) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required string result = 1;
      private Object result_ = "";
      /**
       * <code>required string result = 1;</code>
       */
      public boolean hasResult() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string result = 1;</code>
       */
      public String getResult() {
        Object ref = result_;
        if (!(ref instanceof String)) {
          String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          result_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>required string result = 1;</code>
       */
      public com.google.protobuf.ByteString
          getResultBytes() {
        Object ref = result_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (String) ref);
          result_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string result = 1;</code>
       */
      public Builder setResult(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        result_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string result = 1;</code>
       */
      public Builder clearResult() {
        bitField0_ = (bitField0_ & ~0x00000001);
        result_ = getDefaultInstance().getResult();
        onChanged();
        return this;
      }
      /**
       * <code>required string result = 1;</code>
       */
      public Builder setResultBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        result_ = value;
        onChanged();
        return this;
      }

      // optional string msg = 2;
      private Object msg_ = "";
      /**
       * <code>optional string msg = 2;</code>
       */
      public boolean hasMsg() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional string msg = 2;</code>
       */
      public String getMsg() {
        Object ref = msg_;
        if (!(ref instanceof String)) {
          String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          msg_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>optional string msg = 2;</code>
       */
      public com.google.protobuf.ByteString
          getMsgBytes() {
        Object ref = msg_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (String) ref);
          msg_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string msg = 2;</code>
       */
      public Builder setMsg(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        msg_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string msg = 2;</code>
       */
      public Builder clearMsg() {
        bitField0_ = (bitField0_ & ~0x00000002);
        msg_ = getDefaultInstance().getMsg();
        onChanged();
        return this;
      }
      /**
       * <code>optional string msg = 2;</code>
       */
      public Builder setMsgBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        msg_ = value;
        onChanged();
        return this;
      }

      // optional string body = 3;
      private Object body_ = "";
      /**
       * <code>optional string body = 3;</code>
       */
      public boolean hasBody() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional string body = 3;</code>
       */
      public String getBody() {
        Object ref = body_;
        if (!(ref instanceof String)) {
          String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          body_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>optional string body = 3;</code>
       */
      public com.google.protobuf.ByteString
          getBodyBytes() {
        Object ref = body_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (String) ref);
          body_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string body = 3;</code>
       */
      public Builder setBody(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        body_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string body = 3;</code>
       */
      public Builder clearBody() {
        bitField0_ = (bitField0_ & ~0x00000004);
        body_ = getDefaultInstance().getBody();
        onChanged();
        return this;
      }
      /**
       * <code>optional string body = 3;</code>
       */
      public Builder setBodyBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        body_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:Result)
    }

    static {
      defaultInstance = new Result(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:Result)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_Result_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_Result_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n\017ResultDto.proto\"3\n\006Result\022\016\n\006result\030\001 " +
      "\002(\t\022\013\n\003msg\030\002 \001(\t\022\014\n\004body\030\003 \001(\tB\013B\tResult" +
      "Dto"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_Result_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_Result_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_Result_descriptor,
              new String[] { "Result", "Msg", "Body", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
