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
        //  System.out.println("Thread " +Thread.currentThread().getId()+ " Inserting "+n.getValue());
        //  boolean test = tree.insert(tree, n);
          boolean test = tree.insert(i*i, i);
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
        //  System.out.println("Thread " +Thread.currentThread().getId()+ " Inserting "+n.getValue());
          //boolean test = tree.insert(tree, n);
          boolean test = tree.insert(i*i, i);
          if (!test)
          {
            System.out.println("Could not insert "+n.getKey());
          }
        }
      }
    }
    else
    {
      while(true)
      {
        Node opNode = tree.popMin();
        if (Objects.isNull(opNode))
        {
          break; //System.out.println("Thread "+Thread.currentThread().getId()+" OP Node NULL");
          //continue;
        }
      //  System.out.println("Thread "+Thread.currentThread().getId()+" POPPED OFF "+opNode.getValue());
      }
    }
  }
}
