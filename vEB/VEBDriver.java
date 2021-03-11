public class VEBDriver
{
  public static void main(String[] args)
  {
    SequentialVEB seq = new  SequentialVEB(25);
/*    System.out.println(seq.isMember(seq,3));
    seq.insert(seq,2);
    seq.insert(seq,6);
    System.out.println(seq.successor(seq,2));
    System.out.println(seq.predecessor(seq,6));
    seq.insert(seq,3);
    seq.insert(seq,4);
    System.out.println(seq.successor(seq,2));
    System.out.println(seq.predecessor(seq,6));*/
    System.out.println("\n");
    for (int i = 0; i < 25; i++)
    {
      seq.insert(seq, i);
    }
    for (int i = 0; i < 25; i++)
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
    ParallelVEB parTree = new ParallelVEB(25);
    Thread t1 = new Thread(new VEBThread(25,true,parTree,false));
    Thread t2 = new Thread(new VEBThread(25,false,parTree,false));
    try
    {
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    }
    catch(Exception e){e.printStackTrace();}
    System.out.println(parTree.isMember(parTree,3));
    System.out.println(parTree.successor(parTree,2));
    System.out.println(parTree.predecessor(parTree,6));
    parTree.delete(parTree,4);
    System.out.println(parTree.predecessor(parTree,5));
    System.out.println(parTree.successor(parTree,3));
    System.out.println("Min: "+parTree.getMinimum().getValue());
    System.out.println("Min: "+parTree.getMinimum().getValue());

    System.out.println("\n");
/*    for (int i = 0; i < 25; i++)
    {
      System.out.println("Contains "+i+": "+parTree.isMember(parTree,i));
    }
*/
    Thread t3 = new Thread(new VEBThread(25,true,parTree,true));
    Thread t4 = new Thread(new VEBThread(25,false,parTree,true));
    Thread t5 = new Thread(new VEBThread(25,false,parTree,true));
    long startTime = System.currentTimeMillis();
    try
    {
    t3.start();
    t4.start();
    t5.start();
    t3.join();
    t4.join();
    t5.join();
    }
    catch(Exception e){e.printStackTrace();}
    long endTime = System.currentTimeMillis();
    long runTime = (endTime - startTime);
    double runTimeS = runTime/1000.0;
    System.out.println("Runtime: "+runTimeS+" seconds");
  }
}
