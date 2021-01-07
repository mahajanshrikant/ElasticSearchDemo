package com.solution.Elasticsearch.serviceimpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.solution.Elasticsearch.JsonUtil.IndexNamecreation;
import com.solution.Elasticsearch.JsonUtil.JsonUtil;
import com.solution.Elasticsearch.controller.ElastiscSearchController;
import com.solution.Elasticsearch.model.Movie;

@Service
public class ElastisService implements ElasticSearchInterface {

	private static final Logger logger = LoggerFactory.getLogger(ElastisService.class);

	private RestHighLevelClient client;

	@Autowired
	public ElastisService(RestHighLevelClient client) {
		this.client = client;
	}

	@Override
	public String createIndexWithData(Movie movie) throws IOException {
		IndexRequest request = new IndexRequest(IndexNamecreation.movieIndex);
		request.id(Integer.toString(movie.getMovieid()));
		String jsonMovie = new JsonUtil().convertjavaobjectTojson(movie);
		request.source(jsonMovie, XContentType.JSON);
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		logger.debug("response id", indexResponse.getId());
		return indexResponse.getResult().name();
	}

	@Override
	public List<Movie> searchByQuery(String text) throws Exception {

		SearchResponse response = null;
		try {
			String searchIndex = "moviedata";
			SearchRequest request = new SearchRequest(searchIndex);
			SearchSourceBuilder scb = new SearchSourceBuilder();
			SimpleQueryStringBuilder mcb = QueryBuilders.simpleQueryStringQuery(text);
			scb.query(mcb);
			request.source(scb);
			response = client.search(request, RequestOptions.DEFAULT);
			logger.debug("response", response);
		} catch (IOException ex) {
			logger.debug("Exception in the code", ex);
		}
		return extractSearchResults(response);

	}

	private List<Movie> extractSearchResults(SearchResponse response) {
		List<Movie> resultHolder = new ArrayList<Movie>();
		List<SearchHit> searchHits2 = Arrays.asList(response.getHits().getHits());
		List<Movie> results = new ArrayList<Movie>();
		searchHits2.forEach(hit -> results.add(new JsonUtil().convertJsontoJava(hit.getSourceAsString(), Movie.class)));
		resultHolder.addAll(results);
		return resultHolder;

	}

}
