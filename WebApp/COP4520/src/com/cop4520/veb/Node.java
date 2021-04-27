package com.cop4520.veb;

public class Node
{
  private int val;
  private int key;
  public Node(int x, int k)
  {
    val = x;
    key = k;
  }

  public int getValue()
  {
    return this.val;
  }

  public int getKey()
  {
    return this.key;
  }
}
