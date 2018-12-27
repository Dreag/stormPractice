package edu.yang.storm.trident;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * 进行同一批次各个分区内局部统计
 *
 */
public class SaleSum implements Aggregator<SaleSumState> {

    private Logger logger  = org.slf4j.LoggerFactory.getLogger(SaleSum.class);


    /**
     *
     */
    private static final long serialVersionUID = -6879728480425771684L;

    private int partitionIndex ;
    @Override
    public void prepare(Map conf, TridentOperationContext context) {

        partitionIndex = context.getPartitionIndex();

        //System.err.println(partitionIndex);
        logger.debug("partitionIndex=" + partitionIndex);
        //logger.info(arg0);
        //logger.error(arg0);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public SaleSumState init(Object batchId, TridentCollector collector) {
        return new SaleSumState();
    }

    @Override
    public void aggregate(SaleSumState val, TridentTuple tuple, TridentCollector collector) {

        double oldSaleSum = val.saleSum;

        double price = tuple.getDoubleByField("price");

        double newSaleSum = oldSaleSum + price ;

        val.saleSum = newSaleSum;

    }

    @Override
    public void complete(SaleSumState val, TridentCollector collector) {

//      System.err.println("SaleSum---> partitionIndex=" + this.partitionIndex
//              + ",saleSum=" + val.saleSum);
        collector.emit(new Values(val.saleSum));

    }
}
