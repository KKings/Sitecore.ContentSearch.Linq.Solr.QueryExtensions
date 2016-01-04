// <copyright file="IQueryableExtensions.cs" company="NavigationArts, LLC">
//  Copyright (c) 2015 NavigationArts, LLC All Rights Reserved
// </copyright>
namespace Sitecore.ContentSearch.Linq.Solr.Extensions
{
    using System;
    using System.Linq;
    using Index;
    using Parsing;
    using SolrProvider;

    /// <summary>
    /// Extensions to <see cref="IQueryable"/>
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public static class IQueryableExtensions
    {
        /// <summary>
        /// Extension method to allow the Index to use the Filters on the Facet
        /// </summary>
        /// <param name="source">IQueryable</param>
        /// <param name="context">Search Context. Used to build a FacetLinqToSolrIndex to override default index</param>
        /// <returns>Facet Results</returns>
        public static FacetResults GetFacetsWithFilters<TSource>(this IQueryable<TSource> source, IProviderSearchContext context)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            var linqToSolr = new FacetLinqToSolrIndex<TSource>((SolrSearchContext)context, null);

            return linqToSolr.Execute<FacetResults>((SolrCompositeQuery)((IHasNativeQuery)source).Query);
        }
    }
}
