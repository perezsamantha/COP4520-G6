import java.util.*;
import java.util.concurrent.atomic.AtomicMarkableReference;
public class ParallelVEB
{
  private int universeSize;
  private AtomicMarkableReference<Node> maximum;
  private AtomicMarkableReference<Node> minimum;
  private ParallelVEB summary;
  private ArrayList<ParallelVEB> clusters;

  public ParallelVEB(int size)
  {
    //System.out.println("Size: "+size);
    universeSize = size;
    minimum = new AtomicMarkableReference<Node>(null,false);
    maximum = new AtomicMarkableReference<Node>(null,false);
    //base case
    if (size <= 2)
    {
      // set summary to null as there is no summary for size 2
      summary = null;

      clusters = null;
    }
    else // recursive step
    {
      int numClusters = (int)Math.ceil(Math.sqrt(size));

      // Assign VEB(sqrt(u)) to summary
      summary = new ParallelVEB(numClusters);

      // create array of VEB tree pointers of size sqrt(u)
      clusters = new ArrayList<ParallelVEB>(numClusters);

      // assign VEB(sqrt(u)) to all clusters
      for (int i = 0; i < numClusters; i++)
      {
        clusters.add(new ParallelVEB((int)Math.ceil(Math.sqrt(size))));
      }
    }
  }

  public void setMinimum(int val, int key)
  {
    if (val == -1 && key == -1)
    {
      this.minimum = new AtomicMarkableReference<Node>(null,false);;
    }
    else
    {
      this.minimum = new AtomicMarkableReference<Node>(new Node(val,key),false);
    }

  }

  public void setMaximum(int val, int key)
  {
    if (val == -1 && key == -1)
    {
      this.maximum = new AtomicMarkableReference<Node>(null,false);;
    }
    else
    {
      this.maximum = new AtomicMarkableReference<Node>(new Node(val,key),false);
    }
  }

  private int root(int u)
  {
    return (int)Math.sqrt(u);
  }

  // return cluster number where key is present
  public int high(int x)
  {
    int div = (int)Math.ceil(Math.sqrt(universeSize));
    return x / div;
  }

  // return position of x in cluster
  public int low(int x)
  {
    int div = (int)Math.ceil(Math.sqrt(universeSize));
    return x % div;
  }

  public int getUniverseSize()
  {
    return this.universeSize;
  }

  // this is to return absolute index based on cluster and position number
  public int generateIndex(int x, int y)
  {
    int ru = (int)Math.ceil(Math.sqrt(universeSize));
    return x * ru + y;
  }

  public ParallelVEB getSummary()
  {
    return this.summary;
  }

  public ArrayList<ParallelVEB> getClusters()
  {
    return this.clusters;
  }

  // Returns false if the value is already present in the tree
  // else return true
  public boolean insert(ParallelVEB helper, Node newNode)
  {
    //System.out.println("Inserting "+newNode.getKey());
    // If there are no keys in the tree set min and max equal to this new key

    if (Objects.isNull(helper.getMinimum()))
    {
    //  System.out.println("Checkpoint 1");
      while (true)
      {
        boolean minAttempt = helper.getATMMinimum().compareAndSet(null, newNode, false, false);
        if (!minAttempt)
        {
          if (Objects.isNull(helper.getMinimum()))
          {
            System.out.println("loop1");
            continue;
          }
          else
          {
            System.out.println("Error1: "+newNode.getValue());
            return insert(helper, newNode);
            //return;
          }
        }
        else
        {
          break;
        }

      }
      while (true)
      {
        //System.out.println("Checkpoint 2");
        boolean maxAttempt = helper.getATMMaximum().compareAndSet(null, newNode, false, false);
        if (!maxAttempt)
        {
          if (Objects.isNull(helper.getMaximum()))
          {
            System.out.println("loop2");
            continue;
          }
          else
          {
            System.out.println("Error2");
            return insert(helper, newNode);
            //return;
            //break;
          }
        }
        else
        {
          break;
        }

      }

      //helper.setMinimum(newNode.getValue(), newNode.getKey());
      //helper.setMaximum(newNode.getValue(), newNode.getKey());
    }
    else if (helper.getMinimum().getKey() == newNode.getKey() || helper.getMaximum().getKey() == newNode.getKey())
    {
      //System.out.println("KEY "+newNode.getKey()+" already in this tree");
      return false;
    }
    else
    {
      if (newNode.getKey() < helper.getMinimum().getKey())
      {
      //  System.out.println("Checkpoint 3");
        // If the key is less than current minimum swap values
        // because this minimum is actually the minimum of one of the internal structures
        // we then can continue placing the minimum value at its true position
        Node temp = new Node(newNode.getValue(), newNode.getKey());
        newNode = helper.getMinimum();
          while (true)
          {
            boolean snip =  helper.getATMMinimum().compareAndSet(helper.getMinimum(), temp, false, false);
            if (!snip)
            {
              if (temp.getKey() < helper.getMinimum().getKey())
              {
                System.out.println("loop3");
                continue;
              }
              else
              {
                // if we get here some smaller minimum got inserted and now we need to move on
                System.out.println("Error3");
                newNode = temp;
                break;
              }
            }
            else
            {
              break;
            }

          }
        //helper.setMinimum(temp.get().getValue(), temp.get().getKey());
      }

      if (helper.getUniverseSize() > 2)
      {
        //System.out.println("Checkpoint 4");
        // If no key is present in cluster then insert key to both cluster and summary
        if (this.getMinimumValue(helper.getClusters().get(helper.high(newNode.getKey()))) == -1)
        {
          //System.out.println("Checkpoint 5");
          // set min and max to key as there are no other keys, and we are stopping at this level
          boolean clusterMin = false, clusterMax = false;
          ParallelVEB clusterThing = helper.getClusters().get(helper.high(newNode.getKey()));
          while (true)
          {
            clusterMin = clusterThing.getATMMinimum().compareAndSet(clusterThing.getMinimum(), new Node(newNode.getValue(), helper.low(newNode.getKey())), false, false);
            if (!clusterMin)
            {
              if (this.getMinimumValue(helper.getClusters().get(helper.high(newNode.getKey()))) == -1)
              {
                System.out.println("loop4");
                continue;
              }
              else
              {
                System.out.println("Error4");
                // if we get here, this means there are now nodes in the cluster and we need to restart
                return insert(helper, newNode);
                //return;
              }
            }
            else
            {
              break;
            }

          }
          while (true)
          {
            //System.out.println("Checkpoint 6");
            clusterMax = clusterThing.getATMMaximum().compareAndSet(clusterThing.getMaximum(), new Node(newNode.getValue(), helper.low(newNode.getKey())), false, false);
            if (!clusterMax)
            {
              if (Objects.isNull(clusterThing.getMaximum()))
              {
                System.out.println("loop5");
                continue;
              }
              else
              {
                // if we get here a new maximum was inserted so we just gotta restart
                System.out.println("Error5");
                return insert(helper, newNode);
                //return;
              }
            }
            else
            {
              break;
            }

          }
          insert(helper.getSummary(), new Node(newNode.getValue(),helper.high(newNode.getKey())));
        //  helper.getClusters().get(helper.high(newNode.getKey())).setMinimum(newNode.getValue(),helper.low(newNode.getKey()));
        //  helper.getClusters().get(helper.high(newNode.getKey())).setMaximum(newNode.getValue(),helper.low(newNode.getKey()));
        }
        else
        {
        //  System.out.println("Checkpoint 7");
          // If there are other elements in the tree recursively go deeper
        return insert(helper.getClusters().get(helper.high(newNode.getKey())), new Node(newNode.getValue(),helper.low(newNode.getKey())));
        }
      }

      // set the key as maximum if its greater than current max
      if (Objects.isNull(helper.getMaximum()) || newNode.getKey() > helper.getMaximum().getKey())
      {
      //  System.out.println("Checkpoint 8");
        while(true)
        {
          boolean casMax = helper.getATMMaximum().compareAndSet(helper.getMaximum(), newNode, false, false);
          if (!casMax)
          {
            if (Objects.isNull(helper.getMaximum()) || newNode.getKey() > helper.getMaximum().getKey())
            {
              System.out.println("loop6");
              continue;
            }
            else
            {
              System.out.println("Error6");
              break;
            }
          }
          else
          {
            break;
          }

        }
      }
    }
    return true;
  }
/*
  public synchronized void insert(ParallelVEB helper, Node newNode)
  {s

    // If there are no keys in the tree set min and max equal to this new key
    if (Objects.isNull(helper.getMinimum()))
    {
      helper.setMinimum(newNode.getValue(), newNode.getKey());
      helper.setMaximum(newNode.getValue(), newNode.getKey());
    }
    else
    {
      if (newNode.getKey() < helper.getMinimum().getKey())
      {
        // If the key is less than current minimum swap values
        // because this minimum is actually the minimum of one of the internal structures
        // we then can continue placing the minimum value at its true position
        Node temp = newNode;
        newNode = helper.getMinimum();
        helper.setMinimum(temp.getValue(), temp.getKey());
      }

      if (helper.getUniverseSize() > 2)
      {
        // If no key is present in cluster then insert key to both cluster and summary
        if (this.getMinimumValue(helper.getClusters().get(helper.high(newNode.getKey()))) == -1)
        {
          insert(helper.getSummary(), new Node(newNode.getValue(),helper.high(newNode.getKey())));

          // set min and max to key as there are no other keys, and we are stopping at this level
          helper.getClusters().get(helper.high(newNode.getKey())).setMinimum(newNode.getValue(),helper.low(newNode.getKey()));
          helper.getClusters().get(helper.high(newNode.getKey())).setMaximum(newNode.getValue(),helper.low(newNode.getKey()));
        }
        else
        {
          // If there are other elements in the tree recursively go deeper
          insert(helper.getClusters().get(helper.high(newNode.getKey())), new Node(newNode.getValue(),helper.low(newNode.getKey())));
        }
      }

      // set the key as maximum if its greater than current max
      if (newNode.getKey() > helper.getMaximum().getKey())
      {
        helper.setMaximum(newNode.getValue(), newNode.getKey());
      }
    }
  } */
  public boolean isMember(ParallelVEB helper, int key)
  {

    // if universe size less than key we know for sure we dont have it
    if (key >= helper.getUniverseSize())
    {
      return false;
    }

    // if at any point in our traversal the key is min or maximum value we have it

    if ((!Objects.isNull(helper.getMinimum()) && helper.getMinimum().getKey() == key) || (!Objects.isNull(helper.getMaximum()) && helper.getMaximum().getKey() == key))
    {
      return true;
    }
    else
    {
      // if the size of the tree is 2 the key is not here, as it would have been minimum or maximum
        if (helper.getUniverseSize() == 2)
        {
          return false;
        }
        else
        {
          // recursively call over cluster where the key may be present
          return isMember(helper.getClusters().get(helper.high(key)),helper.low(key));
        }
    }
  }

  public int successor(ParallelVEB helper, int key)
  {
    // if key is - and its successor is present, it's 1. otherwise null.
    if (helper.getUniverseSize() == 2)
    {
      if (key == 0 && !Objects.isNull(helper.getMaximum()) && helper.getMaximum().getKey() == 1)
      {
        return 1;
      }
      else
      {
        return -1;
      }
    }

    // if the key is less than the minimum return the minimum bc it will be the successor
    else if (!Objects.isNull(helper.getMinimum()) && key < helper.getMinimum().getKey())
    {
      return helper.getMinimum().getKey();
    }
    else
    {
      // Find the successor inside the cluter of the key
      // First find the maximum in the cluster
      int maxInCluster = this.getMaximumValue(helper.getClusters().get(helper.high(key)));

      int offset = 0, succCluster = 0;

      // if there is any key (maximum != -1) present in the cluster
      // find successor inside cluster
      if (maxInCluster != -1 && helper.low(key) < maxInCluster)
      {
        offset = this.successor(helper.getClusters().get(helper.high(key)), helper.low(key));

        return helper.generateIndex(helper.high(key), offset);
      }
      // otherwise look for next cluster with at least one key
      else
      {

        succCluster = this.successor(helper.getSummary(), helper.high(key));

        // if there is no cluster with any keys, return null
        if (succCluster == -1)
        {
          return -1;
        }

        // find the minimum in successor cluster which will be the successor
        else
        {
          offset = this.getMinimumValue(helper.getClusters().get(succCluster));

          return helper.generateIndex(succCluster, offset);
        }
      }
    }
  }

  public int predecessor(ParallelVEB helper, int key)
  {
    // if key is 1 predecessor is 0 , otherwise null
    if (helper.getUniverseSize() == 2)
    {
      if (key == 1 && helper.getMinimum().getKey() == 0)
      {
        return 0;
      }
      else
      {
        return -1;
      }
    }

    // If the key is greater than maximum of the tree return max as it will be predecessor
    else if (!Objects.isNull(helper.getMaximum()) && key > helper.getMaximum().getKey())
    {
      return helper.getMaximum().getKey();
    }
    else
    {
      int minInCluster = this.getMinimumValue(helper.getClusters().get(helper.high(key)));

      int offset = 0, predCluster = 0;

      // if any key is present in cluster find predecessor
      if (minInCluster != -1 && helper.low(key) > minInCluster)
      {
        offset = this.predecessor(helper.getClusters().get(helper.high(key)), helper.low(key));

        return helper.generateIndex(helper.high(key), offset);
      }
      // otherwise look for predecessor in summary
      // return indec of predecessor cluster with any key present
      else
      {
        predCluster = this.predecessor(helper.getSummary(), helper.high(key));

        // special case bc of lazy propagation
        if (predCluster == -1)
        {
          if (!Objects.isNull(helper.getMinimum()) && key > helper.getMinimum().getKey())
          {
            return helper.getMinimum().getKey();
          }
          else
          {
            return -1;
          }

        }

        // otherwise find maximum in the predecessor cluster
        else
        {
          offset = this.getMaximumValue(helper.getClusters().get(predCluster));

          return helper.generateIndex(predCluster, offset);
        }
      }
    }
  }

  public void delete(ParallelVEB helper, int key)
  {

    // if only one key is present, its this one so set min and max == 0
    if (helper.getMaximum().getKey() == helper.getMinimum().getKey())
    {
      helper.setMaximum(-1, -1);
      helper.setMinimum(-1, -1);
    }
    // base case if the tree has two keys then we delete one, assign the other to min or max as appropriate
    else if (helper.getUniverseSize() == 2)
    {
      if (key == 0)
      {
        helper.setMinimum(helper.getMaximum().getValue(),helper.getMaximum().getKey());
      }
      else
      {
        helper.setMaximum(helper.getMinimum().getValue(), helper.getMinimum().getKey());
      }
    }

    else
    {
      // find the next bigger key and assign it as minimum
      if (key == helper.getMinimum().getKey())
      {
        int firstCluster = this.getMinimumValue(helper.getSummary());

        int val = helper.getClusters().get(firstCluster).getMinimum().getValue();
        key = helper.generateIndex(firstCluster,this.getMinimumValue(helper.getClusters().get(firstCluster)));

        helper.setMinimum(val, key);
      }

      // now we delete the key
      delete(helper.getClusters().get(helper.high(key)), helper.low(key));

      // if the minimum is -1 we have to delete it from summary
      if (this.getMinimumValue(helper.getClusters().get(helper.high(key))) == -1)
      {
        delete(helper.getSummary(), helper.high(key));

        // if the key is maximum
        if (key == helper.getMaximum().getKey())
        {
          int maxInSummary = this.getMaximumValue(helper.getSummary());

          // if the max value is null, only one key is present so assign min to max
          if (maxInSummary == -1)
          {
            helper.setMaximum(helper.getMinimum().getValue(),helper.getMinimum().getKey());
          }
          else
          {
            // asign global max of tree after deleteing query key
            helper.setMaximum(helper.getClusters().get(maxInSummary).getMaximum().getValue(),helper.generateIndex(maxInSummary, this.getMaximumValue(helper.getClusters().get(maxInSummary))));
          }
        }

      }
      // simply find the new maximum key and set the maximum of tree equal to new maximum
      else if (key == helper.getMaximum().getKey())
      {
        helper.setMaximum(helper.getClusters().get(helper.high(key)).getMaximum().getValue(),helper.generateIndex(helper.high(key),this.getMaximumValue(helper.getClusters().get(helper.high(key)))));
      }
    }
  }

  public int getMinimumValue(ParallelVEB helper)
  {
    if (Objects.isNull(helper.getMinimum()))
    {
      return -1;
    }
    else
    {
      return helper.getMinimum().getKey();
    }
  }

  public Node getMinimum()
  {
    return this.minimum.getReference();
  }

  public int getMaximumValue(ParallelVEB helper)
  {
    if (Objects.isNull(helper.getMaximum()))
    {
      return -1;
    }
    else
    {
      return helper.getMaximum().getKey();
    }
  }

  public Node getMaximum()
  {
    return this.maximum.getReference();
  }

  public AtomicMarkableReference<Node> getATMMinimum()
  {
    return this.minimum;
  }

  public AtomicMarkableReference<Node> getATMMaximum()
  {
    return this.maximum;
  }

  public Node popMin()
  {
    while (true)
    {
        AtomicMarkableReference<Node> minNode = this.getATMMinimum();
        if (Objects.isNull(minNode.getReference()))
        {
          //this means we have no more items in this tree
          return null;
        }
        // uh oh, time to physically remove
        if (minNode.isMarked())
        {
          // obviously this is not going to work, but we are going to come back
          if (Objects.isNull(minNode.getReference()))
          {
            continue;
          }
          else
          {
            delete(this, minNode.getReference().getKey());
            continue;
          }

        }
        //System.out.println("Popping off: "+minNode.getReference().getValue());
        boolean snip = minNode.compareAndSet(minNode.getReference(), minNode.getReference(), false, true);
        if (!snip)
        {
          if (minNode.isMarked())
          {
            // if we get here this means someone else already popped the min so we need to try again;
            continue;
          }
        }

        return minNode.getReference();
      }
  }


}
