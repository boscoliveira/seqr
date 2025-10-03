import React from 'react'
import PropTypes from 'prop-types'
import { Grid } from 'semantic-ui-react'

import { validators } from 'shared/components/form/FormHelpers'
import FormWrapper from 'shared/components/form/FormWrapper'
import { AlignedCheckboxGroup } from 'shared/components/form/Inputs'
import { AwesomeBarFormInput } from 'shared/components/page/AwesomeBar'
import SubmitFormPage from 'shared/components/page/SubmitFormPage'
import { Variant } from 'shared/components/panel/variants/Variants'
import {
  GENE_SEARCH_FREQUENCIES,
  GENOME_VERSION_FIELD,
  GROUPED_VEP_CONSEQUENCES,
  VEP_GROUP_NONSENSE,
  VEP_GROUP_ESSENTIAL_SPLICE_SITE,
  VEP_GROUP_FRAMESHIFT,
  VEP_GROUP_MISSENSE,
  VEP_GROUP_INFRAME,
  VEP_GROUP_SYNONYMOUS,
  VEP_GROUP_EXTENDED_SPLICE_SITE,
} from 'shared/utils/constants'
import { snakecaseToTitlecase } from 'shared/utils/stringUtils'

const validateAnnotations = (value, { annotations }) => (
  value || Object.values(annotations || {}).some(val => val.length) ? undefined : 'At least one consequence filter is required'
)

const CONSEQUENCE_FILEDS = [
  VEP_GROUP_NONSENSE,
  VEP_GROUP_ESSENTIAL_SPLICE_SITE,
  VEP_GROUP_FRAMESHIFT,
  VEP_GROUP_MISSENSE,
  VEP_GROUP_INFRAME,
  VEP_GROUP_SYNONYMOUS,
  VEP_GROUP_EXTENDED_SPLICE_SITE,
].map((group, i) => ({
  name: `annotations.${group}`,
  component: AlignedCheckboxGroup,
  groupLabel: snakecaseToTitlecase(group),
  options: GROUPED_VEP_CONSEQUENCES[group],
  format: value => value || [],
  inline: true,
  validate: i === 0 ? validateAnnotations : undefined,
}))

const FIELDS = [
  { validate: validators.required, ...GENOME_VERSION_FIELD },
  {
    name: 'geneId',
    label: 'Gene',
    control: AwesomeBarFormInput,
    categories: ['genes'],
    fluid: true,
    placeholder: 'Search for gene',
    validate: validators.required,
  },
  ...CONSEQUENCE_FILEDS,
]

const INITIAL_VALUES = { freqs: GENE_SEARCH_FREQUENCIES }

const GeneVariantLookupLayout = ({ fields, uploadStats, onSubmit }) => (
  <Grid divided="vertically">
    <Grid.Row>
      <Grid.Column width={16} textAlign="center">
        <i>
          Lookup up all rare variants is seqr in a given gene, regardless of whether or not they are in your projects.
          <br />
          Variants are only returned if they have a gnomAD Allele Frequency &lt; 3%
          and have a seqr global Allele Count &lt; 3000.
        </i>
      </Grid.Column>
    </Grid.Row>
    <Grid.Row>
      <Grid.Column width={1} />
      <Grid.Column width={14}>
        <FormWrapper
          initialValues={INITIAL_VALUES}
          onSubmit={onSubmit}
          fields={fields}
          noModal
          showErrorPanel
          verticalAlign="top"
        />
      </Grid.Column>
      <Grid.Column width={1} />
    </Grid.Row>
    {uploadStats?.searchedVariants?.map(variant => (
      <Variant key={variant.key} variant={variant} />
    ))}
  </Grid>
)

GeneVariantLookupLayout.propTypes = {
  fields: PropTypes.arrayOf(PropTypes.object),
  uploadStats: PropTypes.object,
  onSubmit: PropTypes.func,
}

const GeneVariantLookup = () => (
  <SubmitFormPage
    fields={FIELDS}
    url="/api/gene_variant_lookup"
    header="Lookup Variants in Gene"
    formClass={GeneVariantLookupLayout}
  />
)

export default GeneVariantLookup
