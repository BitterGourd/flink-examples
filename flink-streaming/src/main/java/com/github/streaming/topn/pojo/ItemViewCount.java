package com.github.streaming.topn.pojo;

/**
 * 商品点击量
 */
public class ItemViewCount {

    /** 商品ID */
    private long itemId;
    /** 窗口结束时间戳 */
    private long windowEnd;
    /** 商品的点击量 */
    private long viewCount;

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.setItemId(itemId);
        result.setWindowEnd(windowEnd);
        result.setViewCount(viewCount);

        return result;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", viewCount=" + viewCount +
                '}';
    }
}
