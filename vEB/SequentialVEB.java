import java.util.*;
public class SequentialVEB
{
  private int universeSize;
  private int maximum;
  private int minimum;
  private SequentialVEB summary;
  private ArrayList<SequentialVEB> clusters;

  public SequentialVEB(int size)
  {
    //System.out.println("Size: "+size);
    universeSize = size;
    minimum = -1;
    maximum = -1;
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
      summary = new SequentialVEB(numClusters);

      // create array of VEB tree pointers of size sqrt(u)
      clusters = new ArrayList<SequentialVEB>(numClusters);

      // assign VEB(sqrt(u)) to all clusters
      for (int i = 0; i < numClusters; i++)
      {
        clusters.add(new SequentialVEB((int)Math.ceil(Math.sqrt(size))));
      }
    }
  }

  public void setMinimum(int val)
  {
    this.minimum = val;
  }

  public void setMaximum(int val)
  {
    this.maximum = val;
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


  public SequentialVEB getSummary()
  {
    return this.summary;
  }

  public ArrayList<SequentialVEB> getClusters()
  {
    return this.clusters;
  }

  public void insert(SequentialVEB helper, int key)
  {

    // If there are no keys in the tree set min and max equal to this new key
    if (helper.getMinimum() == -1)
    {
      helper.setMinimum(key);
      helper.setMaximum(key);
    }
    else
    {
      if (key < helper.getMinimum())
      {
        // If the key is less than current minimum swap values
        // because this minimum is actually the minimum of one of the internal structures
        // we then can continue placing the minimum value at its true position
        int temp = key;
        key = helper.getMinimum();
        helper.setMinimum(temp);
      }

      if (helper.getUniverseSize() > 2)
      {
        // If no key is present in cluster then insert key to both cluster and summary
        if (this.getMinimumValue(helper.getClusters().get(helper.high(key))) == -1)
        {
          insert(helper.getSummary(), helper.high(key));

          // set min and max to key as there are no other keys, and we are stopping at this level
          helper.getClusters().get(helper.high(key)).setMinimum(helper.low(key));
          helper.getClusters().get(helper.high(key)).setMaximum(helper.low(key));
        }
        else
        {
          // If there are other elements in the tree recursively go deeper
          insert(helper.getClusters().get(helper.high(key)), helper.low(key));
        }
      }

      // set the key as maximum if its greater than current max
      if (key > helper.getMaximum())
      {
        helper.setMaximum(key);
      }
    }
// OLD PROTO-VEB code
/*
    if (helper.getUniverseSize() == 2)
    {
      ArrayList<SequentialVEB> clusterTemp = helper.getClusters();
      clusterTemp.set(key, new SequentialVEB(1));
    }
    else
    {
      insert(helper.getClusters().get(helper.high(key)), helper.low(key));

      insert(helper.getSummary(),helper.high(key));
    }
    */
  }

  public boolean isMember(SequentialVEB helper, int key)
  {

    // if universe size less than key we know for sure we dont have it
    if (key >= helper.getUniverseSize())
    {
      return false;
    }

    // if at any point in our traversal the key is min or maximum value we have it
    if (helper.getMinimum() == key || helper.getMaximum() == key)
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
//OLD PROTO CODE
/*
    if (helper.getUniverseSize() == 2)
    {
      try
      {
        SequentialVEB temp = helper.getClusters().get(key);
        if (temp != null)
          return true;
        else
          return false;
      }
      catch(IndexOutOfBoundsException oob)
      {
        System.out.println("WHOOPS");
        return false;
      }
    }
    else
    {
      return isMember(helper.getClusters().get(helper.high(key)), helper.low(key));
    }*/
  }

  public int successor(SequentialVEB helper, int key)
  {
    // if key is - and its successor is present, it's 1. otherwise null.
    if (helper.getUniverseSize() == 2)
    {
      if (key == 0 && helper.getMaximum() == 1)
      {
        return 1;
      }
      else
      {
        return -1;
      }
    }

    // if the key is less than the minimum return the minimum bc it will be the successor
    else if (helper.getMinimum() != -1 && key < helper.getMinimum())
    {
      return helper.getMinimum();
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


  public int predecessor(SequentialVEB helper, int key)
  {
    // if key is 1 predecessor is 0 , otherwise null
    if (helper.getUniverseSize() == 2)
    {
      if (key == 1 && helper.getMinimum() == 0)
      {
        return 0;
      }
      else
      {
        return -1;
      }
    }

    // If the key is greater than maximum of the tree return max as it will be predecessor
    else if (helper.getMaximum() != -1 && key > helper.getMaximum())
    {
      return helper.getMaximum();
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
          if (helper.getMinimum() != -1 && key > helper.getMinimum())
          {
            return helper.getMinimum();
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

  public void delete(SequentialVEB helper, int key)
  {

    // if only one key is present, its this one so set min and max == 0
    if (helper.getMaximum() == helper.getMinimum())
    {
      helper.setMaximum(-1);
      helper.setMinimum(-1);
    }
    // base case if the tree has two keys then we delete one, assign the other to min or max as appropriate
    else if (helper.getUniverseSize() == 2)
    {
      if (key == 0)
      {
        helper.setMinimum(1);
      }
      else
      {
        helper.setMinimum(0);
      }
      helper.setMaximum(helper.getMinimum());
    }

    else
    {
      // find the next bigger key and assign it as minimum
      if (key == helper.getMinimum())
      {
        int firstCluster = this.getMinimumValue(helper.getSummary());

        key = helper.generateIndex(firstCluster,this.getMinimumValue(helper.getClusters().get(firstCluster)));

        helper.setMinimum(key);
      }

      // now we delete the key
      delete(helper.getClusters().get(helper.high(key)), helper.low(key));

      // if the minimum is -1 we have to delete it from summary
      if (this.getMinimumValue(helper.getClusters().get(helper.high(key))) == -1)
      {
        delete(helper.getSummary(), helper.high(key));

        // if the key is maximum
        if (key == helper.getMaximum())
        {
          int maxInSummary = this.getMaximumValue(helper.getSummary());

          // if the max value is null, only one key is present so assign min to max
          if (maxInSummary == -1)
          {
            helper.setMaximum(helper.getMinimum());
          }
          else
          {
            // asign global max of tree after deleteing query key
            helper.setMaximum(helper.generateIndex(maxInSummary, this.getMaximumValue(helper.getClusters().get(maxInSummary))));
          }
        }

      }
      // simply find the new maximum key and set the maximum of tree equal to new maximum
      else if (key == helper.getMaximum())
      {
        helper.setMaximum(helper.generateIndex(helper.high(key),this.getMaximumValue(helper.getClusters().get(helper.high(key)))));
      }
    }
  }

  public int getMinimumValue(SequentialVEB helper)
  {
    return helper.getMinimum();
  }

  public int getMinimum()
  {
    return this.minimum;
  }

  public int getMaximumValue(SequentialVEB helper)
  {
    return helper.getMaximum();
  }

  public int getMaximum()
  {
    return this.maximum;
  }

  public int minimum(SequentialVEB helper)
  {
    // base case
    if (helper.getUniverseSize() == 2)
    {
      if (helper.getClusters().get(0) != null)
      {
        return 0;
      }
      else if (helper.getClusters().get(1) != null)
      {
        return 1;
      }
      return -1;
    }
    else
    {
      // recursively find position in summary
      int minimumCluster = minimum(helper.getSummary());
      int offset;

      if (minimumCluster == -1)
      {
        return -1;
      }
      else
      {
        // find position in minimum cluster
        offset = minimum(helper.getClusters().get(minimumCluster));

        return helper.generateIndex(minimumCluster,offset);
      }
    }
  }

  public int maximum(SequentialVEB helper)
  {
    // base case
    if (helper.getUniverseSize() == 2)
    {
      if (helper.getClusters().get(1) != null)
      {
        return 1;
      }
      else if (helper.getClusters().get(0) != null)
      {
        return 0;
      }
      return -1;
    }
    else
    {
      // recursively find position in summary
      int maximumCluster = maximum(helper.getSummary());
      int offset;

      if (maximumCluster == -1)
      {
        return -1;
      }
      else
      {
        // find position in minimum cluster
        offset = maximum(helper.getClusters().get(maximumCluster));

        return helper.generateIndex(maximumCluster,offset);
      }
    }
  }


}
