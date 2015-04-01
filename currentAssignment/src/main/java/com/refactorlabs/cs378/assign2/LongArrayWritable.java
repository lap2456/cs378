package com.refactorlabs.cs378.assign2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class LongArrayWritable implements Writable {
  private long[] array;
  private int length = 0;

  public LongArrayWritable() {}

  /*
   * Constructor with array as input.
   * @param array: input array
   */
  public LongArrayWritable(long[] array) {
    this.array = array;
    this.length = array.length;
  }

  /*
   * Constructor that creates an empty array of a particular size.
   * @param size: array size
   */
  public LongArrayWritable(int size) {
    array = new long[size];
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.length = in.readInt();
    array = new long[length];
    for (int i = 0; i < length; i++) {
      array[i] = in.readLong();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(length);
    for (int i = 0; i < length; i++) {
      out.writeLong(array[i]);
    }
  }

  /*
   * Returns a reference to the underlying array. Note that the underlying 
   * array may have length longer than the value of size().
   */
  public long[] getArray() {
    return array;
  }

  /*
   * Returns the value at given index.
   * Caller needs to make sure index is within bounds
   * @param index: index position
   */
  public long get(int index) {
    return array[index];
  }

  /*
   * Sets the underlying array.
   * @param array: array that will be filled
   */
  public void setArray(long[] array) {
    if (array == null) {
      this.array = new long[0];
      this.length = 0;
      return;
    }

    this.array = array;
    this.length = array.length;
  }

  /*
   * Sets the value at given index.
   * Caller needs to ensure index is within bounds
   *
   * @param index: position in array
   * @param value: value to set
   */
  public void set(int index, long value) {
    array[index] = value;
  }

  /*
   * Returns the size of the array.
   */
  public int size() {
    return length;
  }

  @Override
  public String toString() {
    return Arrays.toString(array);
  }
}