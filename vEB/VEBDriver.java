import java.util.*;
public class VEBDriver
{
  public static void main(String[] args)
  {
    //SequentialVEB seq = new  SequentialVEB(100);
/*    System.out.println(seq.isMember(seq,3));
    seq.insert(seq,2);
    seq.insert(seq,6);
    System.out.println(seq.successor(seq,2));
    System.out.println(seq.predecessor(seq,6));
    seq.insert(seq,3);
    seq.insert(seq,4);
    System.out.println(seq.successor(seq,2));
    System.out.println(seq.predecessor(seq,6));
    System.out.println("\n");
    for (int i = 0; i < 100; i++)
    {
      seq.insert(seq, i);
    }
    for (int i = 0; i < 100; i++)
    {
      System.out.println("Contains "+i+": "+seq.isMember(seq,i));
    }
    System.out.println("------------------------------");
    /*ParallelVEB par = new ParallelVEB(8);
    System.out.println(par.isMember(par,3));
    Node two = new Node(4,2);
    par.insert(par,two);
    Node three = new Node(9,3);
    par.insert(par,three);
    Node four = new Node(16,4);
    par.insert(par,four);
    Node six = new Node(36,6);
    par.insert(par,six);
    System.out.println(par.isMember(par,3));
    System.out.println(par.successor(par,2));
    System.out.println(par.predecessor(par,6));
    par.delete(par,4);
    System.out.println(par.predecessor(par,6));
    System.out.println(par.successor(par,4));
    System.out.println("Min: "+par.getMinimum().getValue());
    Node one = new Node(1,1);
    par.insert(par, one);
    System.out.println("Min: "+par.getMinimum().getValue());
    System.out.println("------------------------------");
    */
    ParallelVEB parTree = new ParallelVEB(100);
    Thread t1 = new Thread(new VEBThread(100,true,parTree,false));
    Thread t2 = new Thread(new VEBThread(100,false,parTree,false));
    Thread t3 = new Thread(new VEBThread(100,true,parTree,false));
    Thread t4 = new Thread(new VEBThread(100,false,parTree,false));
    Thread t5 = new Thread(new VEBThread(100,true,parTree,true));
    Thread t6 = new Thread(new VEBThread(100,false,parTree,true));
    Thread t7 = new Thread(new VEBThread(100,false,parTree,true));
    long startTime = System.currentTimeMillis();
    try
    {
    t1.start();
    t2.start();
    //t3.start();
    //t4.start();
  //  t5.start();
  //  t6.start();
  //  t7.start();
    t1.join();
    t2.join();
  //  t3.join();
  //  t4.join();
      t5.start();
      t6.start();
    //  t7.start();
    t5.join();
    t6.join();
    //parTree.printList(parTree, 0);
    t3.start();
    t4.start();
    t3.join();
    t4.join();
    //parTree.printList(parTree, 0);
    t7.start();
    t7.join();
  //  t7.join();
    }
    catch(Exception e){e.printStackTrace();}
  //  System.out.println(parTree.isMember(parTree,3));
  //  System.out.println(parTree.successor(parTree,2).getValue());
    //System.out.println(parTree.predecessor(parTree,6));
    //parTree.delete(parTree,4);
    //System.out.println(parTree.predecessor(parTree,5));
    //System.out.println(parTree.successor(parTree,7).getValue());
  //  System.out.println("Min: "+parTree.getMinimum().getValue());
    //System.out.println("Min: "+parTree.getMinimum().getValue());
/*    Node n = parTree.successor(parTree, 0);
    int val = n.getValue();
    for (int i = 1; i < 1000; i++)
    {
      System.out.println("successor: "+val);
      n =  parTree.successor(parTree, i);
      if (Objects.isNull(n))
      {
        val = -1;
      }
      else
      {
        val = n.getValue();
      }

    }
    System.out.println("\n");
/*    for (int i = 0; i < 25; i++)
    {
      System.out.println("Contains "+i+": "+parTree.isMember(parTree,i));
    }
*/

    long endTime = System.currentTimeMillis();
    long runTime = (endTime - startTime);
    double runTimeS = runTime/1000.0;
    System.out.println("Runtime: "+runTimeS+" seconds");
  }
}
