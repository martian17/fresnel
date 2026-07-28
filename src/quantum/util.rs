use nalgebra::{
    SVector,
    SMatrix,
    ComplexField,
};

pub fn outer_product<T, const D: usize>(v: SVector<T, D>) -> SMatrix<T, D, D>
where
    T: ComplexField,
{
    &v * v.adjoint()
}
