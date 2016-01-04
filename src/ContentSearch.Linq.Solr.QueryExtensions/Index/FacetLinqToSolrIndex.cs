// <copyright file="IQueryableExtensions.cs" company="NavigationArts, LLC">
//  Copyright (c) 2015 NavigationArts, LLC All Rights Reserved
// </copyright>
namespace Sitecore.ContentSearch.Linq.Solr.Index
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Xml;
    using Abstractions;
    using Common;
    using Configuration;
    using ContentSearch;
    using ContentSearch.Diagnostics;
    using ContentSearch.Utilities;
    using Linq;
    using Methods;
    using Nodes;
    using Pipelines.GetFacets;
    using Pipelines.IndexingFilters;
    using Pipelines.ProcessFacets;
    using Security;
    using Sitecore.Diagnostics;
    using Solr;
    using SolrNet;
    using SolrNet.Commands.Parameters;
    using SolrNet.Exceptions;
    using SolrProvider;
    using SolrProvider.Logging;

    /// <summary>
    /// Port from Sitecores <see cref="LinqToSolrIndex~TItem"/>
    /// Adds the FilterValues into QueryOptions for SolrNet
    /// </summary>
    public class FacetLinqToSolrIndex<TItem> : LinqToSolrIndex<TItem>
    {

        private readonly SolrSearchContext _context;
        private readonly string _cultureCode;
        public FieldNameTranslator PublicFieldNameTranslator { get; set; }
        private readonly ISettings _settings;
        private readonly IContentSearchConfigurationSettings _contentSearchSettings;

        public FacetLinqToSolrIndex(SolrSearchContext context, IExecutionContext executionContext)
            : base(context, executionContext)
        {

            Assert.ArgumentNotNull(context, "context");
            _context = context;
            _settings = context.Index.Locator.GetInstance<ISettings>();
            _contentSearchSettings = context.Index.Locator.GetInstance<IContentSearchConfigurationSettings>();
            var executionContext1 = Parameters.ExecutionContext as CultureExecutionContext;
            var culture = executionContext1 == null ? CultureInfo.GetCultureInfo(Settings.DefaultLanguage) : executionContext1.Culture;
            _cultureCode = culture.TwoLetterISOLanguageName;
            ((SolrFieldNameTranslator)Parameters.FieldNameTranslator).AddCultureContext(culture);
            PublicFieldNameTranslator = FieldNameTranslator;
        }

        public override TResult Execute<TResult>(SolrCompositeQuery compositeQuery)
        {
            if (EnumerableLinq.ShouldExecuteEnumerableLinqQuery(compositeQuery))
                return EnumerableLinq.ExecuteEnumerableLinqQuery<TResult>(compositeQuery);
            if (typeof (TResult).IsGenericType &&
                typeof (TResult).GetGenericTypeDefinition() == typeof (SearchResults<>))
            {
                var resultType = typeof (TResult).GetGenericArguments()[0];
                var solrQueryResults = Execute(compositeQuery, resultType);
                var type = typeof (SolrSearchResults<>).MakeGenericType(resultType);
                var methodInfo =
                    GetType()
                        .GetMethod("ApplyScalarMethods", BindingFlags.Instance | BindingFlags.NonPublic)
                        .MakeGenericMethod(typeof (TResult), resultType);
                var selectMethod = FacetLinqToSolrIndex<TItem>.GetSelectMethod(compositeQuery);

                var instance = ReflectionUtility.CreateInstance(type, (object) _context, (object) solrQueryResults,
                    (object) selectMethod, (object) compositeQuery.ExecutionContexts,
                    (object) compositeQuery.VirtualFieldProcessors);
                return (TResult) methodInfo.Invoke(this, new object[3]
                {
                    compositeQuery,
                    instance,
                    solrQueryResults
                });
            }

            compositeQuery.Methods.Add(new GetFacetsMethod());

            var solrQueryResults1 = Execute(compositeQuery, typeof (TResult));
            var selectMethod1 = FacetLinqToSolrIndex<TItem>.GetSelectMethod(compositeQuery);
            var processedResults = new SolrSearchResults<TResult>(_context, solrQueryResults1, selectMethod1, compositeQuery.ExecutionContexts, compositeQuery.VirtualFieldProcessors);

            return ApplyScalarMethods<TResult, TResult>(compositeQuery, processedResults, solrQueryResults1);
        }

        internal SolrQueryResults<Dictionary<string, object>> Execute(SolrCompositeQuery compositeQuery, Type resultType)
        {
            var options = new QueryOptions();
            if (compositeQuery.Methods != null)
            {
                var list1 =
                    compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.Select).Select(m => (SelectMethod) m).ToList();
                if (list1.Any())
                {
                    foreach (
                        var str in
                            list1.SelectMany(selectMethod => (IEnumerable<string>) selectMethod.FieldNames))
                        options.Fields.Add(str.ToLowerInvariant());
                    options.Fields.Add("_uniqueid");
                    options.Fields.Add("_datasource");
                }
                var list2 =
                    compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.GetResults).Select(m => (GetResultsMethod) m).ToList();
                if (list2.Any())
                {
                    if (options.Fields.Count > 0)
                    {
                        options.Fields.Add("score");
                    }
                    else
                    {
                        options.Fields.Add("*");
                        options.Fields.Add("score");
                    }
                }
                var list3 =
                    compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.OrderBy).Select(m => (OrderByMethod) m).ToList();
                if (list3.Any())
                {
                    foreach (var orderByMethod in list3)
                    {
                        var field = orderByMethod.Field;
                        options.AddOrder(new SortOrder(field,
                            orderByMethod.SortDirection == SortDirection.Ascending ? Order.ASC : Order.DESC));
                    }
                }
                int startIdx;
                int maxHits;
                GetMaxHits(compositeQuery, _contentSearchSettings.SearchMaxResults(), out startIdx, out maxHits);
                var list4 =
                    compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.Skip).Select(m => (SkipMethod) m).ToList();
                if (list4.Any())
                {
                    var num = list4.Sum(skipMethod => skipMethod.Count);
                    options.Start = new int?(num);
                }
                var list5 =
                    compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.Take).Select(m => (TakeMethod) m).ToList();
                if (list5.Any())
                {
                    var num = list5.Sum(takeMethod => takeMethod.Count);
                    options.Rows = new int?(num);
                }
                var list6 =
                    compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.Count).Select(m => (CountMethod) m).ToList();
                if (compositeQuery.Methods.Count == 1 && list6.Any())
                    options.Rows = new int?(0);
                var list7 =
                    compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.Any).Select(m => (AnyMethod) m).ToList();
                if (compositeQuery.Methods.Count == 1 && list7.Any())
                    options.Rows = new int?(0);
                var list8 =
                    compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.GetFacets).Select(m => (GetFacetsMethod) m).ToList();
                if (compositeQuery.FacetQueries.Count > 0 &&
                    (list8.Any() ||
                     list2.Any()))
                {
                    foreach (
                        var facetQuery in
                            GetFacetsPipeline.Run(
                                new GetFacetsArgs(null,
                                    compositeQuery.FacetQueries,
                                    _context.Index.Configuration.VirtualFieldProcessors,
                                    _context.Index.FieldNameTranslator)).FacetQueries.ToHashSet())
                    {
                        if (facetQuery.FieldNames.Any())
                        {
                            var minimumResultCount = facetQuery.MinimumResultCount;

                            if (facetQuery.FieldNames.Count() == 1)
                            {
                                var fieldNameTranslator = FieldNameTranslator as SolrFieldNameTranslator;

                                var fieldName = facetQuery.FieldNames.First();

                                if (fieldNameTranslator != null &&
                                    fieldName == fieldNameTranslator.StripKnownExtensions(fieldName)
                                    &&
                                    _context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName) == null)
                                {
                                    fieldName =
                                        fieldNameTranslator.GetIndexFieldName(
                                            fieldName.Replace("__", "!").Replace("_", " ").Replace("!", "__"), true);
                                }

                                /**
                                 * ******* UPDATED HERE *******
                                 * If any FilterValues passed into the Facets, add them to the FilterValues AND the Facet On
                                 * have them excluded
                                 */
                                if (facetQuery.FilterValues.Any())
                                {
                                    options.AddFilterQueries(new LocalParams { { "tag", fieldName } } + new SolrQueryInList(fieldName, facetQuery.FilterValues.Cast<string>()));

                                    options.AddFacets(
                                        new SolrFacetFieldQuery(new LocalParams {{"ex", fieldName}} + fieldName));
                                }
                                else
                                {
                                    options.AddFacets((ISolrFacetQuery) new SolrFacetFieldQuery(fieldName)
                                    {
                                        MinCount = minimumResultCount
                                    });
                                }
                            }

                            if (facetQuery.FieldNames.Count() > 1)
                                options.AddFacets((ISolrFacetQuery) new SolrFacetPivotQuery()
                                {
                                    Fields = (ICollection<string>) new string[1]
                                    {
                                        string.Join(",", facetQuery.FieldNames)
                                    },
                                    MinCount = minimumResultCount
                                });
                        }
                    }
                    if (!list2.Any())
                    {
                        options.Rows = new int?(0);
                    }
                }
            }
            if (compositeQuery.Filter != null)
                options.AddFilterQueries((ISolrQuery) compositeQuery.Filter);
            options.AddFilterQueries((ISolrQuery) new SolrQueryByField("_indexname", _context.Index.Name));
            if (!_settings.DefaultLanguage().StartsWith(_cultureCode))
                options.AddFilterQueries((ISolrQuery) new SolrQueryByField("_language", _cultureCode + "*")
                {
                    Quoted = false
                });
            var loggingSerializer = new SolrLoggingSerializer();
            var q = loggingSerializer.SerializeQuery(compositeQuery.Query);
            var solrSearchIndex = _context.Index as SolrSearchIndex;
            try
            {
                if (!options.Rows.HasValue)
                    options.Rows = new int?(_contentSearchSettings.SearchMaxResults());
                SearchLog.Log.Info("Query - " + q, null);
                SearchLog.Log.Info(
                    "Serialized Query - ?q=" + q + "&" +
                    string.Join("&",
                        loggingSerializer.GetAllParameters(options).Select(p => string.Format("{0}={1}", p.Key, p.Value)).ToArray()),
                    null);
                return SolrOperations.Query(q, options);
            }
            catch (Exception ex)
            {
                if (!(ex is SolrConnectionException) && !(ex is SolrNetException))
                {
                    throw;
                }
                else
                {
                    var message = ex.Message;
                    if (ex.Message.StartsWith("<?xml"))
                    {
                        var xmlDocument = new XmlDocument();
                        xmlDocument.LoadXml(ex.Message);
                        var xmlNode1 =
                            xmlDocument.SelectSingleNode("/response/lst[@name='error'][1]/str[@name='msg'][1]");
                        var xmlNode2 =
                            xmlDocument.SelectSingleNode(
                                "/response/lst[@name='responseHeader'][1]/lst[@name='params'][1]/str[@name='q'][1]");
                        if (xmlNode1 != null && xmlNode2 != null)
                        {
                            SearchLog.Log.Error(
                                string.Format("Solr Error : [\"{0}\"] - Query attempted: [{1}]",
                                    xmlNode1.InnerText, xmlNode2.InnerText), null);
                            return new SolrQueryResults<Dictionary<string, object>>();
                        }
                    }
                    Log.Error(message, this);
                    return new SolrQueryResults<Dictionary<string, object>>();
                }
            }
        }

        private ISolrOperations<Dictionary<string, object>> SolrOperations
        {
            get
            {
                var solrSearchIndex = _context.Index as SolrSearchIndex;

                if (solrSearchIndex != null)
                {
                    return typeof(SolrSearchIndex)
                        .GetProperty("SolrOperations", BindingFlags.NonPublic | BindingFlags.Instance)
                        .GetValue(solrSearchIndex) as ISolrOperations<Dictionary<string, object>>;
                }
                return null;
            }
        }

        private int GetMaxHits(SolrCompositeQuery query, int maxDoc, out int startIdx, out int maxHits)
        {
            var list = query.Methods != null ? new List<QueryMethod>(query.Methods) : new List<QueryMethod>();
            list.Reverse();
            var modifierScalarMethod = GetMaxHitsModifierScalarMethod(query.Methods);
            startIdx = 0;
            var num1 = maxDoc;
            var num2 = num1;
            var num3 = num1;
            var num4 = 0;
            foreach (var queryMethod in list)
            {
                switch (queryMethod.MethodType)
                {
                    case QueryMethodType.Skip:
                        var count = ((SkipMethod)queryMethod).Count;
                        if (count > 0)
                        {
                            startIdx += count;
                            continue;
                        }
                        continue;
                    case QueryMethodType.Take:
                        num4 = ((TakeMethod)queryMethod).Count;
                        if (num4 <= 0)
                        {
                            num1 = startIdx++;
                            continue;
                        }
                        if (num4 > 1 && modifierScalarMethod != null && modifierScalarMethod.MethodType == QueryMethodType.First)
                            num4 = 1;
                        if (num4 > 1 && modifierScalarMethod != null && modifierScalarMethod.MethodType == QueryMethodType.Any)
                            num4 = 1;
                        if (num4 > 2 && modifierScalarMethod != null && modifierScalarMethod.MethodType == QueryMethodType.Single)
                            num4 = 2;
                        num1 = startIdx + num4 - 1;
                        if (num1 > num2)
                        {
                            num1 = num2;
                            continue;
                        }
                        if (num2 < num1)
                        {
                            num2 = num1;
                            continue;
                        }
                        continue;
                    default:
                        continue;
                }
            }
            if (num3 == num1)
            {
                num4 = -1;
                if (modifierScalarMethod != null && modifierScalarMethod.MethodType == QueryMethodType.First)
                    num4 = 1;
                if (modifierScalarMethod != null && modifierScalarMethod.MethodType == QueryMethodType.Any)
                    num4 = 1;
                if (modifierScalarMethod != null && modifierScalarMethod.MethodType == QueryMethodType.Single)
                    num4 = 2;
                if (num4 >= 0)
                {
                    num1 = startIdx + num4 - 1;
                    if (num1 > num2)
                        num1 = num2;
                    else if (num2 < num1)
                        ;
                }
            }
            if (num3 == num1 && startIdx == 0 && (modifierScalarMethod != null && modifierScalarMethod.MethodType == QueryMethodType.Count))
                ;
            maxHits = num4;
            return maxHits;
        }

        private static SelectMethod GetSelectMethod(SolrCompositeQuery compositeQuery)
        {
            var list = compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.Select).Select(m => (SelectMethod)m).ToList();
            if (list.Count() != 1)
                return null;
            return list[0];
        }

        private QueryMethod GetMaxHitsModifierScalarMethod(List<QueryMethod> methods)
        {
            if (methods.Count == 0)
                return null;
            var queryMethod = methods.First();
            switch (queryMethod.MethodType)
            {
                case QueryMethodType.Any:
                case QueryMethodType.Count:
                case QueryMethodType.First:
                case QueryMethodType.Last:
                case QueryMethodType.Single:
                    return queryMethod;
                default:
                    return null;
            }
        }

        private TResult ApplyScalarMethods<TResult, TDocument>(SolrCompositeQuery compositeQuery, SolrSearchResults<TDocument> processedResults, SolrQueryResults<Dictionary<string, object>> results)
        {
            var queryMethod = compositeQuery.Methods.First();
            object obj;
            switch (queryMethod.MethodType)
            {
                case QueryMethodType.All:
                    obj = true;
                    break;
                case QueryMethodType.Any:
                    obj = processedResults.Any() ? 1 : 0;
                    break;
                case QueryMethodType.Count:
                    obj = !compositeQuery.Methods.Any(i =>
                    {
                        if (i.MethodType != QueryMethodType.Take)
                            return i.MethodType == QueryMethodType.Skip;
                        return true;
                    }) ? processedResults.Count() : (object)results.Count();
                    break;
                case QueryMethodType.ElementAt:
                    obj = !((ElementAtMethod)queryMethod).AllowDefaultValue ? processedResults.ElementAt(((ElementAtMethod)queryMethod).Index) : (object)processedResults.ElementAtOrDefault(((ElementAtMethod)queryMethod).Index);
                    break;
                case QueryMethodType.First:
                    obj = !((FirstMethod)queryMethod).AllowDefaultValue ? processedResults.First() : (object)processedResults.FirstOrDefault();
                    break;
                case QueryMethodType.Last:
                    obj = !((LastMethod)queryMethod).AllowDefaultValue ? processedResults.Last() : (object)processedResults.LastOrDefault();
                    break;
                case QueryMethodType.Single:
                    obj = !((SingleMethod)queryMethod).AllowDefaultValue ? processedResults.Single() : (object)processedResults.SingleOrDefault();
                    break;
                case QueryMethodType.GetResults:
                    var searchHits = processedResults.GetSearchHits();
                    var facetResults = FormatFacetResults(processedResults.GetFacets(), compositeQuery.FacetQueries);
                    obj = ReflectionUtility.CreateInstance(typeof(TResult), (object)searchHits, (object)processedResults.NumberFound, (object)facetResults);
                    break;
                case QueryMethodType.GetFacets:
                    obj = FormatFacetResults(processedResults.GetFacets(), compositeQuery.FacetQueries);
                    break;
                default:
                    throw new InvalidOperationException("Invalid query method");
            }
            return (TResult)Convert.ChangeType(obj, typeof(TResult));
        }

        private FacetResults FormatFacetResults(Dictionary<string, ICollection<KeyValuePair<string, int>>> facetResults, List<FacetQuery> facetQueries)
        {
            var fieldNameTranslator = _context.Index.FieldNameTranslator as SolrFieldNameTranslator;
            var dictionary = ProcessFacetsPipeline.Run(new ProcessFacetsArgs(facetResults, facetQueries, facetQueries, _context.Index.Configuration.VirtualFieldProcessors, fieldNameTranslator));
            foreach (var facetQuery in facetQueries)
            {
                var originalQuery = facetQuery;
                if (originalQuery.FilterValues != null && originalQuery.FilterValues.Any() && dictionary.ContainsKey(originalQuery.CategoryName))
                {
                    var collection = dictionary[originalQuery.CategoryName];

                    /**
                     * ****** UPDATE ******
                     * Allow ALL Facets to come back from Solr
                     */ 
                    dictionary[originalQuery.CategoryName] = collection.Select(cv => cv).ToList();
                }
            }
            var facetResults1 = new FacetResults();
            foreach (var keyValuePair in dictionary)
            {
                if (fieldNameTranslator != null)
                {
                    var key = keyValuePair.Key;
                    string name;
                    if (key.Contains(","))
                        name = fieldNameTranslator.StripKnownExtensions(key.Split(new char[1]
                        {
                            ','
                        }, StringSplitOptions.RemoveEmptyEntries));
                    else
                        name = fieldNameTranslator.StripKnownExtensions(key);
                    var values = keyValuePair.Value.Select(v => new FacetValue(v.Key, v.Value));
                    facetResults1.Categories.Add(new FacetCategory(name, values));
                }
            }
            return facetResults1;
        }
        
        internal struct SolrSearchResults<TElement>
        {
            private readonly SolrSearchContext _context;
            private readonly SolrQueryResults<Dictionary<string, object>> _searchResults;
            private readonly SolrIndexConfiguration _solrIndexConfiguration;
            private readonly IIndexDocumentPropertyMapper<Dictionary<string, object>> _mapper;
            private readonly SelectMethod _selectMethod;
            private readonly IEnumerable<IExecutionContext> _executionContexts;
            private readonly IEnumerable<IFieldQueryTranslator> _virtualFieldProcessors;
            private readonly int _numberFound;

            public int NumberFound
            {
                get
                {
                    return _numberFound;
                }
            }

            public SolrSearchResults(SolrSearchContext context, SolrQueryResults<Dictionary<string, object>> searchResults, SelectMethod selectMethod, IEnumerable<IExecutionContext> executionContexts, IEnumerable<IFieldQueryTranslator> virtualFieldProcessors)
            {
                _context = context;
                _solrIndexConfiguration = (SolrIndexConfiguration)_context.Index.Configuration;
                _selectMethod = selectMethod;
                _virtualFieldProcessors = virtualFieldProcessors;
                _executionContexts = executionContexts;
                _numberFound = searchResults.NumFound;
                _searchResults = SolrSearchResults<TElement>.ApplySecurity(searchResults, context.SecurityOptions, context.Index.Locator.GetInstance<ICorePipeline>(), context.Index.Locator.GetInstance<IAccessRight>(), ref _numberFound);
                var executionContext = _executionContexts != null ? _executionContexts.FirstOrDefault(c => c is OverrideExecutionContext<IIndexDocumentPropertyMapper<Dictionary<string, object>>>) as OverrideExecutionContext<IIndexDocumentPropertyMapper<Dictionary<string, object>>> : null;
                _mapper = (executionContext != null ? executionContext.OverrideObject : null) ?? _solrIndexConfiguration.IndexDocumentPropertyMapper;
            }

            private static SolrQueryResults<Dictionary<string, object>> ApplySecurity(SolrQueryResults<Dictionary<string, object>> solrQueryResults, SearchSecurityOptions options, ICorePipeline pipeline, IAccessRight accessRight, ref int numberFound)
            {
                if (!options.HasFlag(SearchSecurityOptions.DisableSecurityCheck))
                {
                    var hashSet = new HashSet<Dictionary<string, object>>();
                    foreach (var dictionary in solrQueryResults.Where(searchResult => searchResult != null))
                    {
                        object obj1;
                        if (dictionary.TryGetValue("_uniqueid", out obj1))
                        {
                            object obj2;
                            dictionary.TryGetValue("_datasource", out obj2);
                            if (OutboundIndexFilterPipeline.CheckItemSecurity(pipeline, accessRight, new OutboundIndexFilterArgs((string)obj1, (string)obj2)))
                            {
                                hashSet.Add(dictionary);
                                numberFound = numberFound - 1;
                            }
                        }
                    }
                    foreach (var dictionary in hashSet)
                        solrQueryResults.Remove(dictionary);
                }
                return solrQueryResults;
            }

            public TElement ElementAt(int index)
            {
                if (index < 0 || index > _searchResults.Count)
                    throw new IndexOutOfRangeException();
                return _mapper.MapToType<TElement>(_searchResults[index], _selectMethod, _virtualFieldProcessors, _executionContexts, _context.SecurityOptions);
            }

            public TElement ElementAtOrDefault(int index)
            {
                if (index < 0 || index > _searchResults.Count)
                    return default(TElement);
                return _mapper.MapToType<TElement>(_searchResults[index], _selectMethod, _virtualFieldProcessors, _executionContexts, _context.SecurityOptions);
            }

            public bool Any()
            {
                return _numberFound > 0;
            }

            public long Count()
            {
                return _numberFound;
            }

            public TElement First()
            {
                if (_searchResults.Count < 1)
                    throw new InvalidOperationException("Sequence contains no elements");
                return ElementAt(0);
            }

            public TElement FirstOrDefault()
            {
                if (_searchResults.Count < 1)
                    return default(TElement);
                return ElementAt(0);
            }

            public TElement Last()
            {
                if (_searchResults.Count < 1)
                    throw new InvalidOperationException("Sequence contains no elements");
                return ElementAt(_searchResults.Count - 1);
            }

            public TElement LastOrDefault()
            {
                if (_searchResults.Count < 1)
                    return default(TElement);
                return ElementAt(_searchResults.Count - 1);
            }

            public TElement Single()
            {
                if (Count() < 1L)
                    throw new InvalidOperationException("Sequence contains no elements");
                if (Count() > 1L)
                    throw new InvalidOperationException("Sequence contains more than one element");
                return _mapper.MapToType<TElement>(_searchResults[0], _selectMethod, _virtualFieldProcessors, _executionContexts, _context.SecurityOptions);
            }

            public TElement SingleOrDefault()
            {
                if (Count() == 0L)
                    return default(TElement);
                if (Count() == 1L)
                    return _mapper.MapToType<TElement>(_searchResults[0], _selectMethod, _virtualFieldProcessors, _executionContexts, _context.SecurityOptions);
                throw new InvalidOperationException("Sequence contains more than one element");
            }

            public IEnumerable<SearchHit<TElement>> GetSearchHits()
            {
                foreach (var document in _searchResults)
                {
                    var score = -1f;
                    object scoreObj;
                    if (document.TryGetValue("score", out scoreObj) && scoreObj is float)
                        score = (float)scoreObj;
                    yield return new SearchHit<TElement>(score, _mapper.MapToType<TElement>(document, _selectMethod, _virtualFieldProcessors, _executionContexts, _context.SecurityOptions));
                }
            }

            public IEnumerable<TElement> GetSearchResults()
            {
                foreach (var document in _searchResults)
                    yield return _mapper.MapToType<TElement>(document, _selectMethod, _virtualFieldProcessors, _executionContexts, _context.SecurityOptions);
            }

            public Dictionary<string, ICollection<KeyValuePair<string, int>>> GetFacets()
            {
                var facetFields = _searchResults.FacetFields;
                var facetPivots = _searchResults.FacetPivots;
                var dictionary = facetFields.ToDictionary(x => x.Key, x => x.Value);
                if (facetPivots.Count > 0)
                {
                    foreach (var keyValuePair in facetPivots)
                        dictionary[keyValuePair.Key] = Flatten(keyValuePair.Value, string.Empty);
                }
                return dictionary;
            }

            private ICollection<KeyValuePair<string, int>> Flatten(IEnumerable<Pivot> pivots, string parentName)
            {
                var hashSet = new HashSet<KeyValuePair<string, int>>();
                foreach (var pivot in pivots)
                {
                    if (parentName != string.Empty)
                        hashSet.Add(new KeyValuePair<string, int>(parentName + "/" + pivot.Value, pivot.Count));
                    if (pivot.HasChildPivots)
                        hashSet.UnionWith(Flatten(pivot.ChildPivots, pivot.Value));
                }
                return hashSet;
            }
        }
    }
}
