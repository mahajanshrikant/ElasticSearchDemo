package com.solution.Elasticsearch.controller;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.solution.Elasticsearch.model.Movie;
import com.solution.Elasticsearch.serviceimpl.ElastisService;
import com.solution.Elasticsearch.serviceimpl.MovieServiceInterface;

@RestController
public class ElastiscSearchController {

	private static final Logger logger = LoggerFactory.getLogger(ElastiscSearchController.class);
	private ElastisService service;

	@Autowired
	MovieServiceInterface movieServiceInterface;

	@Autowired
	public ElastiscSearchController(ElastisService service) {
		this.service = service;
	}

	@RequestMapping(value = "/getAllmovieData", method = RequestMethod.GET)
	public List<Movie> getAllMovieData() throws IOException {
		List<Movie> result = movieServiceInterface.getallMovieData();
		logger.debug("response data{}", result);
		for (Movie movieloop : result) {
			service.createIndexWithData(movieloop);
		}

		return result;
	}

	@RequestMapping(value = "/movies", method = RequestMethod.GET)
	public List<Movie> searchByQuery(@RequestParam("q") String query) throws Exception {
		logger.debug("query data{}", query);
		return service.searchByQuery(query);

	}

}
