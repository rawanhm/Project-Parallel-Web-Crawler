package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler  {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final List<Pattern> ignoredUrls;
  private final int maxDepth;
  private final PageParserFactory parserFactory;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @IgnoredUrls List<Pattern> ignoredUrls,
          @MaxDepth int maxDepth,
          PageParserFactory parserFactory) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.ignoredUrls = ignoredUrls;
    this.maxDepth = maxDepth;
    this.parserFactory = parserFactory;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) throws Exception{


    Instant deadline = clock.instant().plus(timeout);
    Map<String, Integer> counts = new ConcurrentHashMap<>();
    Set<String> visitedUrls = new ConcurrentSkipListSet<>();

    for (String url : startingUrls) {
      SubRecursiveAction SubRecursiveAction = new SubRecursiveAction(url, deadline, maxDepth, counts, visitedUrls, clock, ignoredUrls, parserFactory);
      pool.invoke(SubRecursiveAction);
    }
    CrawlResult CrawlResult;
    if (counts.isEmpty()) {
     CrawlResult=  new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
       return CrawlResult;
    }
  CrawlResult = new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
    return  CrawlResult;

  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

}

//_________________________________________own custom task class_________________________________________
public class SubRecursiveAction extends RecursiveAction {
  private final String url;
  private final Instant deadline;
  private final int maxDepth;
  private final Map<String, Integer> counts;
  private final Set<String> visitedUrls;
  private final Clock clock;
  private final List<Pattern> ignoredUrls;
  private final PageParserFactory parserFactory;

  public SubRecursiveAction(String url, Instant deadline, int maxDepth, Map<String, Integer> counts, Set<String> visitedUrls, Clock clock,
                            List<Pattern> ignoredUrls,
                            PageParserFactory parserFactory) {
    this.url = url;
    this.deadline = deadline;
    this.maxDepth = maxDepth;
    this.counts = counts;
    this.visitedUrls = visitedUrls;
    this.clock = clock;
    this.ignoredUrls = ignoredUrls;
    this.parserFactory = parserFactory;
  }

  @Override
  protected void compute() {
    if (maxDepth == 0 || clock.instant().isAfter(deadline)) return;
    for (Pattern pattern : ignoredUrls) {
      if (pattern.matcher(url).matches()) return;
    }
    if (!visitedUrls.add(url)) return;

    PageParser.Result result = null;
    try {
      result = parserFactory.get(url).parse();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
      counts.compute(e.getKey(), (k, v) -> (v == null) ? e.getValue() : e.getValue() + v);
    }

    List<SubRecursiveAction> subtasks = new ArrayList<>();
    SubRecursiveAction task;
    for (String link : result.getLinks()) {
      task = new SubRecursiveAction(link, deadline, maxDepth - 1, counts, visitedUrls, clock, ignoredUrls,
              parserFactory);
      subtasks.add(task);
    }
    invokeAll(subtasks);
  }

  }
