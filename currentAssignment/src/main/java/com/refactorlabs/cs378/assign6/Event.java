/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.refactorlabs.cs378.assign6;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Event extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.refactorlabs.cs378.assign6\",\"fields\":[{\"name\":\"event_type\",\"type\":{\"type\":\"enum\",\"name\":\"EventType\",\"symbols\":[\"CHANGE\",\"CLICK\",\"CONTACT_FORM_STATUS\",\"EDIT\",\"SHARE\",\"SHOW\",\"SUBMIT\",\"VISIT\"]}},{\"name\":\"event_subtype\",\"type\":{\"type\":\"enum\",\"name\":\"EventSubtype\",\"symbols\":[\"CONTACT_FORM\",\"MARKET_REPORT\"]}},{\"name\":\"event_time\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public com.refactorlabs.cs378.assign6.EventType event_type;
  @Deprecated public com.refactorlabs.cs378.assign6.EventSubtype event_subtype;
  @Deprecated public java.lang.CharSequence event_time;

  /**
   * Default constructor.
   */
  public Event() {}

  /**
   * All-args constructor.
   */
  public Event(com.refactorlabs.cs378.assign6.EventType event_type, com.refactorlabs.cs378.assign6.EventSubtype event_subtype, java.lang.CharSequence event_time) {
    this.event_type = event_type;
    this.event_subtype = event_subtype;
    this.event_time = event_time;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return event_type;
    case 1: return event_subtype;
    case 2: return event_time;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: event_type = (com.refactorlabs.cs378.assign6.EventType)value$; break;
    case 1: event_subtype = (com.refactorlabs.cs378.assign6.EventSubtype)value$; break;
    case 2: event_time = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'event_type' field.
   */
  public com.refactorlabs.cs378.assign6.EventType getEventType() {
    return event_type;
  }

  /**
   * Sets the value of the 'event_type' field.
   * @param value the value to set.
   */
  public void setEventType(com.refactorlabs.cs378.assign6.EventType value) {
    this.event_type = value;
  }

  /**
   * Gets the value of the 'event_subtype' field.
   */
  public com.refactorlabs.cs378.assign6.EventSubtype getEventSubtype() {
    return event_subtype;
  }

  /**
   * Sets the value of the 'event_subtype' field.
   * @param value the value to set.
   */
  public void setEventSubtype(com.refactorlabs.cs378.assign6.EventSubtype value) {
    this.event_subtype = value;
  }

  /**
   * Gets the value of the 'event_time' field.
   */
  public java.lang.CharSequence getEventTime() {
    return event_time;
  }

  /**
   * Sets the value of the 'event_time' field.
   * @param value the value to set.
   */
  public void setEventTime(java.lang.CharSequence value) {
    this.event_time = value;
  }

  /** Creates a new Event RecordBuilder */
  public static com.refactorlabs.cs378.assign6.Event.Builder newBuilder() {
    return new com.refactorlabs.cs378.assign6.Event.Builder();
  }
  
  /** Creates a new Event RecordBuilder by copying an existing Builder */
  public static com.refactorlabs.cs378.assign6.Event.Builder newBuilder(com.refactorlabs.cs378.assign6.Event.Builder other) {
    return new com.refactorlabs.cs378.assign6.Event.Builder(other);
  }
  
  /** Creates a new Event RecordBuilder by copying an existing Event instance */
  public static com.refactorlabs.cs378.assign6.Event.Builder newBuilder(com.refactorlabs.cs378.assign6.Event other) {
    return new com.refactorlabs.cs378.assign6.Event.Builder(other);
  }
  
  /**
   * RecordBuilder for Event instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Event>
    implements org.apache.avro.data.RecordBuilder<Event> {

    private com.refactorlabs.cs378.assign6.EventType event_type;
    private com.refactorlabs.cs378.assign6.EventSubtype event_subtype;
    private java.lang.CharSequence event_time;

    /** Creates a new Builder */
    private Builder() {
      super(com.refactorlabs.cs378.assign6.Event.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.refactorlabs.cs378.assign6.Event.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing Event instance */
    private Builder(com.refactorlabs.cs378.assign6.Event other) {
            super(com.refactorlabs.cs378.assign6.Event.SCHEMA$);
      if (isValidValue(fields()[0], other.event_type)) {
        this.event_type = data().deepCopy(fields()[0].schema(), other.event_type);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.event_subtype)) {
        this.event_subtype = data().deepCopy(fields()[1].schema(), other.event_subtype);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.event_time)) {
        this.event_time = data().deepCopy(fields()[2].schema(), other.event_time);
        fieldSetFlags()[2] = true;
      }
    }

    /** Gets the value of the 'event_type' field */
    public com.refactorlabs.cs378.assign6.EventType getEventType() {
      return event_type;
    }
    
    /** Sets the value of the 'event_type' field */
    public com.refactorlabs.cs378.assign6.Event.Builder setEventType(com.refactorlabs.cs378.assign6.EventType value) {
      validate(fields()[0], value);
      this.event_type = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'event_type' field has been set */
    public boolean hasEventType() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'event_type' field */
    public com.refactorlabs.cs378.assign6.Event.Builder clearEventType() {
      event_type = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'event_subtype' field */
    public com.refactorlabs.cs378.assign6.EventSubtype getEventSubtype() {
      return event_subtype;
    }
    
    /** Sets the value of the 'event_subtype' field */
    public com.refactorlabs.cs378.assign6.Event.Builder setEventSubtype(com.refactorlabs.cs378.assign6.EventSubtype value) {
      validate(fields()[1], value);
      this.event_subtype = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'event_subtype' field has been set */
    public boolean hasEventSubtype() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'event_subtype' field */
    public com.refactorlabs.cs378.assign6.Event.Builder clearEventSubtype() {
      event_subtype = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'event_time' field */
    public java.lang.CharSequence getEventTime() {
      return event_time;
    }
    
    /** Sets the value of the 'event_time' field */
    public com.refactorlabs.cs378.assign6.Event.Builder setEventTime(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.event_time = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'event_time' field has been set */
    public boolean hasEventTime() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'event_time' field */
    public com.refactorlabs.cs378.assign6.Event.Builder clearEventTime() {
      event_time = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public Event build() {
      try {
        Event record = new Event();
        record.event_type = fieldSetFlags()[0] ? this.event_type : (com.refactorlabs.cs378.assign6.EventType) defaultValue(fields()[0]);
        record.event_subtype = fieldSetFlags()[1] ? this.event_subtype : (com.refactorlabs.cs378.assign6.EventSubtype) defaultValue(fields()[1]);
        record.event_time = fieldSetFlags()[2] ? this.event_time : (java.lang.CharSequence) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
