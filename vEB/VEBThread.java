import java.util.*;
public class VEBThread implements Runnable
{
  int total;
  boolean odds;
  ParallelVEB tree;
  boolean toPrint;
  public VEBThread(int x, boolean b, ParallelVEB pv, boolean rw)
  {
    total = x; odds = b; tree = pv; toPrint = rw;
  }
  public void run()
  {
    if (!toPrint)
    {
      if (!odds)
      {
        for (int i = 0; i < total; i+=2)
        {
          Node n = new Node(i*i, i);
          boolean test = tree.insert(tree, n);
          if (!test)
          {
            System.out.println("Could not insert "+n.getKey());
          }
        }
      }
      else
      {
        for (int i = 1; i < total; i+=2)
        {
          Node n = new Node(i*i, i);
          boolean test = tree.insert(tree, n);
          if (!test)
          {
            System.out.println("Could not insert "+n.getKey());
          }
        }
      }
    }
    else
    {
      for (int i = 0; i < total; i++)
      {
        Node opNode = tree.popMin();
        if (Objects.isNull(opNode))
        {
          break;
        }
        //System.out.println("Thread "+Thread.currentThread().getId()+" POPPED OFF "+opNode.getValue());
      }
    }
  }
}
